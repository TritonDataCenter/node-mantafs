// Copyright 2014 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var events = require('events');
var fs = require('fs');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var levelup = require('levelup');
var LRU = require('lru-cache');
var LineStream = require('lstream');
var manta = require('manta');
var once = require('once');
var uuid = require('node-uuid');

var DirStream = require('./readdir_stream');
var ErrnoError = require('./errors').ErrnoError;
var utils = require('./utils');



///--- Globals

var sprintf = util.format;

var DIRTY_POLL_INTERVAL = parseInt(process.env.FS_POLL_INTERVAL || 10, 10);

var CACHE_DIR_NAME = 'fscache';
var DB_NAME = 'mantafs.db';

var DIRTY_KEY_FMT = ':d:%s';
var FHANDLE_KEY_FMT = '::fh:%s';
var FILES_KEY_FMT = ':f:%s';
var FNAME_KEY_FMT = '::fn:%s';



///--- Helpers

function bytes(mb) {
    assert.number(mb, 'megabytes');

    return (Math.floor(mb * 1024  *1024));
}


// We know that all values going into set/get are the results of
// Manta.info() calls, so we do our own age management on top of
// the LRU TTL (based on cache-control)
function createLruCache(opts) {
    assert.object(opts, 'options');

    var lru = LRU(opts);

    var _get = lru.get.bind(lru);

    function lru_get(k) {
        var v = _get(k);
        if (v &&
            v._manta &&
            v._manta.headers &&
            v._manta.headers['cache-control']) {
            /* JSSTYLED */
            var cc = v._manta.headers['cache-control'].split(/\s*,\s*/);
            var ma;
            if (cc.some(function (p) {
                var ok = false;
                if (/max-age/.test(p)) {
                    ok = true;
                    ma = parseInt(p.split('=')[1], 10);
                }
                return (ok);
            })) {
                var now = new Date().getTime();
                if ((now - v.atime.getTime()) > (ma * 1000)) {
                    lru.del(k);
                    v = null;
                }
            }
        }
        return (v);
    }

    lru.get = lru_get;

    return (lru);
}


function errno(err, syscall) {
    // TODO
    var code = 'EIO';

    return (new ErrnoError(code, syscall || 'leveldb', err));
}


function MB(b) {
    assert.number(b, 'bytes');

    return (Math.floor(b / 1024 /1024));
}



///--- API

function Cache(opts) {
    assert.object(opts, 'options');
    // assert.number(opts.dirty, 'options.dirty');
    // assert.number(opts.dirtyAge, 'options.dirtyAge');
    assert.object(opts.log, 'options.log');
    assert.string(opts.location, 'options.location');
    assert.number(opts.size, 'options.size');
    assert.number(opts.ttl, 'options.ttl');

    events.EventEmitter.call(this, opts);

    this.cache = createLruCache({
        dispose: function onDispose(k, v) {
            if (v._no_evict)
                return;

            console.log('dispose: ' + k);
        },
        max: Infinity, // hacky - but we just want LRU bookeeping
        maxAge: opts.ttl
    });

    this.current_size = 0;
    this.db = null;
    this.log = opts.log.child({
        component: 'MantaFsCache',
        location: path.normalize(opts.location)
    }, true);
    this.location = path.normalize(opts.location);
    this.max_size = opts.size;
    this.ttl = opts.ttl;

    this._mantafs_cache = true; // MDB flag

    this.open();

    // LRU eviction is asynchronous, and sometimes triggered when
    // we don't want it (like in readdir/stat). So we have to do
    // some book-keeping so that evictions/uploads skip entries we know
    // are "hot"
    //
    // this.dirtyAge = opts.dirtyAge * 1000;
    // this._no_upload = {};
    // this._timers = {};
}
util.inherits(Cache, events.EventEmitter);
module.exports = Cache;



/**
 * When a caller knows that there is already somebody else performing
 * a download, rather than stomping all over each other, we just stack
 * up callers and wait for the data they need in the local cache file.
 *
 * This method simply indicates whether the data is in the dirty file
 * or not.
 *
 * `path` is the local cache file, not the manta path.
 * `pos` is the last byte of data to wait for.
 */
Cache.prototype.dataInCache = function dataInCache(p, pos, cb) {
    assert.string(p, 'path');
    assert.number(pos, 'position');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var ok = false;

    log.trace('dataInCache(%s, %d): entered', p, pos);

    this.get(p, function onDbGet(err, info) {
        if (err) {
            log.trace(err, 'dataInCache(%s): failed', p);
            cb(err);
        } else {
            fs.stat(p, function onStatDone(err2, stats) {
                if (err2) {
                    if (err2.code !== 'ENOENT') {
                        log.trace(err2, 'dataInCache(%s): failed', p);
                        cb(err2);
                        return;
                    }
                } else {
                    ok = stats.size === info.size || info.size >= pos;
                }

                log.trace('dataInCache(%s, %d): %j', p, pos, ok);
                cb(null, ok, info);
            });
        }
    });
};


Cache.prototype.setEvict = function setEvict(p, val) {
    assert.string(p, 'path');
    assert.optionalBool(val, 'value');

    var k = sprintf(FILES_KEY_FMT, p);
    var stats = this.cache.get(k);

    if (stats) {
        if (val === false) {
            stats._no_evict = true;
        } else if (stats._no_evict) {
            delete stats._no_evict;
        }
    }
};


Cache.prototype.inFlight = function inFlight(p, val) {
    assert.string(p, 'path');
    assert.optionalBool(val, 'value');

    var k = sprintf(FILES_KEY_FMT, p);
    var stats = this.cache.get(k);

    if (stats && val !== undefined) {
        if (val === true) {
            stats._in_flight = true;
        } else if (val === false && stats._in_flight) {
            delete stats._in_flight;
        }
    }

    return ((stats || {})._in_flight || false);
};


Cache.prototype.fhandle = function fhandle(fh, cb) {
    assert.string(fh, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var k = sprintf(FNAME_KEY_FMT, fh);
    var log = this.log;

    log.trace('fhandle(%s): entered', fh);
    this.db.get(k, function onDbGet(err, val) {
        if (err) {
            log.trace(err, 'fhandle(%s): error', fh);
            cb(errno(err, 'fhandle'));
        } else {
            log.trace('fhandle(%s): done => %s', fh, val);
            cb(null, val);
        }
    });
};


Cache.prototype.lookup = function lookup(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    p = manta.path(p);
    cb = once(cb);

    var k = sprintf(FHANDLE_KEY_FMT, p);
    var log = this.log;

    log.trace('lookup(%s): entered', p);
    this.db.get(k, function onDbGet(err, val) {
        if (err) {
            log.trace(err, 'lookup(%s): error', p);
            cb(errno(err, 'lookup'));
        } else {
            log.trace('lookup(%s): done => %s', p, val);
            cb(null, val);
        }
    });
};


Cache.prototype.has = function has(p) {
    return (this.cache.has(sprintf(FILES_KEY_FMT, manta.path(p))));
};

Cache.prototype.get = function get(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    p = manta.path(p);
    cb = once(cb);

    var key = sprintf(FILES_KEY_FMT, p);
    var log = this.log;
    var self = this;

    log.trace('get(%s): entered', p);

    if (this.cache.has(key)) {
        var _stats = this.cache.get(key);
        setImmediate(function onGetFileCacheDone() {
            log.trace('get(%s): done (cached) => %j', p, _stats);
            cb(null, _stats);
        });
        return;
    }

    this.db.get(key, function onDBGet(err, stats) {
        if (err) {
            log.trace(err, 'get(%s): failed', p);
            cb(err.notFound ? null : errno(err), null);
        } else {
            log.trace('get(%s): done => %j', p, stats);
            self.cache.set(key, stats);
            cb(null, stats);
        }
    });
};


Cache.prototype.put = function put(p, info, cb) {
    assert.string(p, 'path');
    assert.object(info, 'info');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('put(%s, %j): entered', p, info);

    this.get(p, function onGet(err, val) {
        if (err) {
            cb(err);
            return;
        } if (val) {
            info._fhandle = val._fhandle;
            info._cacheFile = val._cacheFile;
        } else {
            info._fhandle = uuid.v4();
            info._cacheFile =
                path.join(self.location, 'fscache', info._fhandle);
        }

        p = manta.path(p);
        var key = sprintf(FILES_KEY_FMT, p);
        var k1 = sprintf(FHANDLE_KEY_FMT, p);
        var k2 = sprintf(FNAME_KEY_FMT, info._fhandle);

        self.db.batch()
            .put(key, info)
            .put(k1, info._fhandle)
            .put(k2, p)
            .write(function onBatchWrite(err2) {
                if (err2) {
                    log.trace(err, 'put(%s): failed', p);
                    cb(errno(err2));
                } else {
                    log.trace('put(%s): done', p);
                    self.cache.set(key, info);
                    cb(null, info);
                }
            });
    });
};


Cache.prototype.del = function del(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('del(%s): entered', p);
    this.get(p, function (err, info) {
        if (err) {
            log.trace(err, 'del(%s): failed', p);
            cb(err);
            return;
        }

        p = manta.path(p);
        var key = sprintf(FILES_KEY_FMT, p);
        var fhk = sprintf(FHANDLE_KEY_FMT, p);
        var fnk = sprintf(FNAME_KEY_FMT, info._fhandle);
        self.db.batch()
            .del(key)
            .del(fhk)
            .del(fnk)
            .write(function onBatchDel(err2) {
                if (err2) {
                    log.trace(err, 'del(%s): failed', p);
                    cb(errno(err2));
                } else {
                    log.trace('del(%s): done', p);
                    self.cache.del(key);
                    cb(null);
                }
            });
    });
};

// Cache.prototype.read = function read(p, cb) {
//     assert.string(p, 'path');
//     assert.func(cb, 'callback');

//     cb = once(cb);

//     var log = this.log;

//     log.trace('read(%s): entered', p);

//     this.get(p, function onDBGet(err, val) {
//         if (err) {
//             log.trace(err, 'read(%s): failed (DB)', p);
//             cb(errno(err, 'read'));
//             return;
//         }

//         var fstream = fs.createReadStream(val._cacheFile);

//         fstream.once('error', function onError(err2) {
//             cb(err2.code === 'ENOENT' ? null : err2, null);
//         });

//         fstream.once('open', function onOpen() {
//             log.trace('read(%s): ready (returning stream)', p);
//             cb(null, fstream);
//         });
//     });
// };


Cache.prototype.readdir = function readdir(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;

    log.trace('readdir(%s): entered', p);

    this.get(p, function onDBGet(err, val) {
        if (err) {
            log.trace(err, 'readdir(%s): failed (DB)', p);
            cb(errno(err, 'readdir'));
            return;
        }

        var fstream = fs.createReadStream(val._cacheFile);

        fstream.once('error', function onError(err2) {
            cb(err2.code === 'ENOENT' ? null : err2, null);
        });
        fstream.once('open', function onOpen() {
            var dstream = new DirStream({encoding: 'utf8'});
            var lstream = new LineStream({encoding: 'utf8'});

            fstream.pipe(lstream).pipe(dstream);
            log.trace('readdir(%s): done (returning stream)', p);
            cb(null, dstream);
        });
    });
};


/**
 * When a caller creates dirty data, we need to record an entry in the (cached)
 * parent directory.  It is assumed that the caller knows the parent directory
 * is already in the cache. If it isn't, you get an error.
 *
 */
Cache.prototype.dirtyParent = function dirtyParent(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var dir = path.dirname(p);
    var log = this.log;
    var self = this;

    log.trace('dirtyParent(%s): entered', p);

    this.get(dir, function (p_err, p_info) {
        if (p_err) {
            log.trace(p_err, 'dirtyParent(%s): failed', p);
            cb(p_err);
            return;
        }

        self.get(p, function (err, info) {
            if (err) {
                log.trace(err, 'dirtyParent(%s): failed', p);
                cb(err);
                return;
            }

            var data = '\n' + JSON.stringify({
                name: path.basename(p),
                type: info.extension === 'directory' ? 'directory' : 'object',
                mtime: new Date().toJSON()
            }) + '\n';
            var fname = p_info._cacheFile;

            // Note here if the directory was *not* fully cached, we're
            // ensuring the semantics of `ls` are to return only the dirty data,
            // which is likely not what the user wants, but for speed this is
            // what we currently do. If it turns out this is a problem, we'll
            // want the API layer to ensure the parent dir is fully brought down
            fs.appendFile(fname, data, {encoding: 'utf8'}, function (err1) {
                if (err1) {
                    log.trace(err1, 'dirtyParent(%s): failed (append)', p);
                    cb(err1);
                } else {
                    log.trace('dirtyParent(%s): done', p);
                    cb();
                }
            });
        });
    });
};


Cache.prototype.undirtyParent = function undirtyParent(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var name = path.basename(manta.path(p, true));

    log.trace('undirtyParent(%s): entered', p);

    this.get(path.dirname(manta.path(p, true)), function (perr, pinfo) {
        if (perr || !(pinfo || {})._cacheFile) {
            log.trace(perr, 'undirtyParent(%s): parent not cached, done', p);
            cb();
            return;
        }

        fs.stat(pinfo._cacheFile, function (serr, stats) {
            if (serr) {
                if (serr.code === 'ENOENT') {
                    log.trace('undirtyParent(%s): parent not cached, done', p);
                    cb();
                } else {
                    log.trace(serr, 'undirtyParent(%s): failed', p);
                    cb(serr);
                }
                return;
            }

            var lstream = new LineStream({
                encoding: 'utf8'
            });
            var rstream = fs.createReadStream(pinfo._cacheFile, {
                encoding: 'utf8'
            });
            var tmp = pinfo._cacheFile + '.tmp';
            var wstream = fs.createWriteStream(tmp, {
                encoding: 'utf8'
            });

            function _cb(err) {
                if (err) {
                    log.trace(err, 'undirtyParent(%s): failed', p);
                    cb(err);
                } else {
                    log.trace('undirtParent(%s): done', p);
                }
            }
            lstream.once('error', _cb);
            rstream.once('error', _cb);
            wstream.once('error', _cb);

            // We want to write everything except for the current key
            lstream.on('data', function onDirectoryLine(line) {
                if (!line)
                    return;

                try {
                    var data = JSON.parse(line);
                    if (data.name === name)
                        return;
                } catch (e) {
                    log.trace(e, 'undirtyParent(%s): cache data corruption', p);
                    _cb(e);
                    return;
                }

                if (!wstream.write(line + '\n')) {
                    wstream.once('drain', lstream.resume.bind(lstream));
                    lstream.pause();
                }
            });

            lstream.once('end', function onDirectoryEnd() {
                wstream.once('finish', function onFlush() {
                    fs.rename(tmp, pinfo._cacheFile, function onRename(rerr) {
                        if (rerr) {
                            log.trace(rerr, 'undirtyParent(%s): failed', p);
                            cb(rerr);
                        } else {
                            log.trace('undirtyParent(%s): done', p);
                            cb();
                        }
                    });
                });
                wstream.end();
            });

            rstream.pipe(lstream);
        });
    });
};


/**
 * When a caller knows that there is already somebody else performing
 * a download, rather than stomping all over each other, we just stack
 * up callers and wait for the data they need in the local cache file.
 *
 * This method performs that process of waiting for the data in
 * the local (dirty) cache file.
 *
 * `path` is the local cache file, not the manta path.
 * `pos` is the last byte of data to wait for.
 * `info` is an HTTP HEAD object
 */
Cache.prototype.wait = function wait(p, info, pos, cb) {
    assert.string(p, 'path').
    assert.object(info, 'info');
    assert.number(pos, 'position');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('wait(%s, %d, %d): entered', p, info.size, pos);

    // fs.watch() seems to be unreliable across platforms (like Mac),
    // so we do this the ghetto way :(
    (function poll() {
        self.dataInCache(p, info, pos, function onIsReady(err, ready) {
            if (err) {
                log.trace(err, 'wait(%s, %d, %d): failed', p, info.size, pos);
                cb(err);
            } else if (!ready) {
                setTimeout(poll, DIRTY_POLL_INTERVAL);
            } else {
                log.trace('wait(%s, %d, %d): ready', p, info.size, pos);
                cb();
            }
        });
    })();
};


//-- Open/Close/other stuff not mainline

Cache.prototype.open = function open() {
    var db_location = path.join(this.location, DB_NAME);
    var location = path.join(this.location, CACHE_DIR_NAME);
    var log = this.log;
    var self = this;

    log.debug('open: entered');

    utils.ensureAndStat(location, function onReady(err, stats) {
        if (err) {
            log.fatal(err, 'initialization of %s failed', location);
            self.emit('error', err);
            return;
        }

        if (stats.statvfs.availableMB < MB(self.max_size)) {
            log.warn('%s has %dMB available. Using as max size',
                     location, stats.statvfs.availableMB);

            self.max_size = bytes(stats.statvfs.availableMB);
        }

        self.db = levelup(db_location, {
            valueEncoding: 'json'
        });

        self.db.on('error', self.emit.bind(self, 'error'));
        self.db.once('ready', function onDatabase() {
            /* JSSTYLED */
            var d_re = /^::dirty:/;
            var keys = self.db.createReadStream();
            /* JSSTYLED */
            var f_re = /^::files:/;
            var tmp = {};

            // We happen to know (since it's a BTree) that we're going
            // to see the keys for "dirty" before "files", so if it's dirty,
            // we just keep a table of them, and overwrite the files as we
            // see them. Janky, but it works.
            keys.on('data', function onDbEntry(data) {
                log.trace('open: DB entry: %j', data);
                if (d_re.test(data.key)) {
                    tmp[data.value.name] = data.value;
                    // TODO - add to dirty queue
                    // self._timers[data.key] =
                    //     setTimeout(self._upload.bind(self, data.key),
                    //                self.dirtyAge);
                } else if (f_re.test(data.key)) {
                    var k = data.key.split(':').pop();
                    var v = data.value;
                    if (tmp[k]) {
                        v.size = tmp[k].size;
                        delete tmp[k];
                    }

                    self.size += v.size;
                    self.cache.set(data.key, v);
                }
            });
            keys.once('error', self.emit.bind(self, 'error'));
            keys.once('end', function () {
                log.debug('open: done');
                self.emit('ready');
            });
        });
    });
};


Cache.prototype.close = function close(cb) {
    assert.optionalFunc(cb, 'callback');

    var log = this.log;
    var self = this;

    var _cb = once(function (err) {
        if (err) {
            log.error(err, 'close: failed');
            if (cb) {
                cb(err);
            } else {
                self.emit('error', err);
            }
        } else {
            log.debug(err, 'close: done');
            self.emit('close');
            if (cb)
                cb();
        }
    });

    log.debug('close: entered');

    if (this.db) {
        this.db.close(_cb);
    } else {
        _cb();
    }
};


Cache.prototype.toString = function toString() {
    var str = '[object ' +
        this.constructor.name + '<' +
        'location=' + this.location + ', ' +
        'max_files=' + this.max_files + ', ' +
        'max_size=' + this.max_size + ', ' +
        'ttl=' + this.ttl + ', ' +
        'current_size=' + this.current_size + '>]';

    return (str);
};
