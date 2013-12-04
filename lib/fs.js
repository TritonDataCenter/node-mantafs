// Copyright 2013 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var events = require('events');
var fs = require('fs');
var path = require('path');
var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var crc32 = require('sse4_crc32');
var levelup = require('levelup');
var libuuid = require('libuuid');
var LineStream = require('lstream');
var LRU = require('lru-cache');
var once = require('once');

var errors = require('./errors');
var utils = require('./utils');



///--- Globals

var sprintf = util.format;
var ErrnoError = errors.ErrnoError;
var xlateDBError = errors.xlateDBError;
var xlateMantaError = errors.xlateMantaError;

var FHANDLE_KEY_FMT = '::fhandles:%s';
var FNAME_KEY_FMT = '::fnames:%s';
var FILES_KEY_FMT = '::files:%s';



///--- Helpers

function MB(b) {
    assert.number(b, 'bytes');

    return (Math.floor(b / 1024 /1024));
}


function bytes(mb) {
    assert.number(mb, 'megabytes');

    return (Math.floor(mb * 1024  *1024));
}


function init(thisp) {
    assert.object(thisp, 'MantaFs');
    assert.ok(thisp instanceof MantaFs, 'MantaFs');

    var log = thisp.log;

    utils.ensureAndStat(thisp.path + '/fscache', function onReady(err, stats) {
        if (err) {
            log.fatal(err, 'initialization of %s failed', thisp.path);
            thisp.emit('error', err);
            return;
        }

        if (stats.availableMB < MB(thisp.size)) {
            log.warn('%s has %dMB available. Using as max size',
                     thisp.path, stats.availableMB);

            thisp.max_size = bytes(stats.availableMB);
        }

        thisp.db = levelup(thisp.path + '/manta.db', {
            valueEncoding: 'json'
        });

        thisp.db.on('error', thisp.emit.bind(thisp, 'error'));
        thisp.db.once('ready', function onDatabase() {
            // TODO: read back any persisted state from leveldb
            // thisp.size = ...;
            thisp.emit('ready');
        });
    });
}


function _false() {
    return (false);
}

function _true() {
    return (true);
}

// Converts a manta.info() into an fs.Stats object
function mantaToStats(_path, info) {
    assert.string(_path, 'path');
    assert.object(info, 'info');

    var stats = new fs.Stats();
    stats.dev = crc32.calculate(path.dirname(_path));
    stats.ino = crc32.calculate(info.etag);
    if (info.extension === 'directory') {
        stats.nlink = parseInt(info.headers['result-set-size'], 10);
        stats.isFile = _false;
        stats.isDirectory = _true;
        stats.mode = 0755;
    } else {
        stats.nlink = 1;
        stats.isFile = _true;
        stats.isDirectory = _false;
        stats.mode = 0644;
    }
    stats.uid = -2;    // TODO: this is Mac-only
    stats.gid = -2;    // TODO: this is Mac-only
    stats.rdev = 0;
    stats.size = info.size;
    stats.atime = new Date();
    stats.mtime = new Date(info.headers['last-modified']);
    stats.ctime = stats.mtime;

    stats.isBlockDevice = _false;
    stats.isCharacterDevice = _false;
    stats.isSymbolicLink = _false;
    stats.isFIFO = _false;
    stats.isSocket = _false;

    stats._manta = info;
    stats._path = _path;

    return (stats);
}



///--- API

/**
 * Constructor
 *
 * This creates a MantaFs instance, and assumes you are going to pass it in a
 * valid node-manta handle. Additionally, you pass in the following options:
 *
 * - files<Number>: Maximum number of files to cache
 * - log<Bunyan>: log handle
 * - path<String>: file system root to cache in
 * - sizeMB<Number>: Maximum number of megabytes to have resident on disk
 * - ttl<Number>: Maximum default age of files (in seconds)
 *
 * Once you instantiate this, wait for the `ready` event.
 */
function MantaFs(opts) {
    assert.object(opts, 'options');
    assert.number(opts.files, 'options.files');
    assert.object(opts.log, 'options.log');
    assert.object(opts.manta, 'options.manta');
    assert.string(opts.path, 'options.path');
    assert.number(opts.sizeMB, 'options.sizeMB');
    assert.number(opts.ttl, 'options.ttl');

    events.EventEmitter.call(this, opts);

    this.cache = LRU({
        evict: this._evict.bind(this),
        max: opts.files,
        maxAge: opts.ttl * 1000
    });
    this.log = opts.log.child({component: 'MantaFs'}, true);
    this.manta = opts.manta;
    this.max_size = bytes(opts.sizeMB);
    this.path = path.normalize(opts.path);
    this.size = 0;
    this.ttl = opts.ttl * 1000;

    init(this);
}
util.inherits(MantaFs, events.EventEmitter);


/**
 * Shuts down the cache, and closes the internal leveldb.
 *
 * You can optionally pass a callback. The callback will receive any error
 * encountered during closing as the first argument.
 *
 * If no callback is provided, the fs instance will emit `close` or `error`.
 *
 */
MantaFs.prototype.close = function close(cb) {
    assert.optionalFunc(cb, 'callback');
    var self = this;

    this.db.close(function onDbClose(err) {
        if (err) {
            if (cb) {
                cb(err);
            } else {
                self.emit('error', err);
            }
        } else {
            if (cb) {
                cb();
            } else {
                self.emit('close');
            }
        }
    });
};


MantaFs.prototype.lookup = function lookup(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);
    _path = path.normalize(_path);

    var k = sprintf(FHANDLE_KEY_FMT, _path);
    this.db.get(k, function onDbGet(err, val) {
        if (err) {
            cb(xlateDBError(err));
        } else {
            cb(null, val);
        }
    });
};


MantaFs.prototype.fhandle_to_path = function fhandle_to_path(_fhandle, cb) {
    assert.string(_fhandle, 'fhandle');
    assert.func(cb, 'callback');

    cb = once(cb);

    var k = sprintf(FNAME_KEY_FMT, _fhandle);

    this.db.get(k, function (err, val) {
        cb(err, val);
        return;
    });
};


MantaFs.prototype.read = function read(fd, buffer, off, len, pos, cb) {
    assert.number(fd, 'fd');
    assert.buffer(buffer, 'buffer');
    assert.number(off, 'offset');
    assert.number(len, 'length');
    assert.number(pos, 'position');
    assert.func(cb, 'callback');

};


/**
 * Implementation of fs.readdir() [really readdir(3)].
 *
 */
MantaFs.prototype.readdir = function readdir(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);
    _path = path.normalize(_path);

    var names = [];
    var self = this;

    // We always stat(2) first, so we ensure that info is cached
    this.stat(_path, function onStat(s_err, stats) {
        if (s_err) {
            cb(s_err);
            return;
        }

        if (!stats.isDirectory()) {
            cb(new ErrnoError('ENOTDIR', 'readdir'));
            return;
        }

        if (stats._cacheFile) {
            var inf = fs.createReadStream(stats._cacheFile);
            var lstream = new LineStream({encoding: 'utf8'});
            inf.once('error', cb);
            lstream.once('error', cb);
            lstream.on('data', function (l) {
                if (l) {
                    try {
                        var entry = JSON.parse(l);
                        names.push(entry.name);
                    } catch (e) {
                        self.log.error({
                            err: e,
                            line: l,
                            stats: stats,
                            path: _path
                        }, 'readdir: cache data corruption');
                        lstream.removeAllListeners('data');
                        lstream.removeAllListeners('end');
                        lstream.resume();
                        cb(e);
                    }
                }
            });

            lstream.once('end', cb.bind(this, null, names));
            inf.pipe(lstream);
            return;
        }

        // cache miss
        var cacheFile = path.join(self.path, stats._fhandle);
        var out = fs.createWriteStream(cacheFile);
        out.once('error', cb);
        out.once('open', function onFileOpen() {
            self.manta.ls(_path, function onLsStart(ls_err, res) {
                if (ls_err) {
                    cb(xlateMantaError(ls_err));
                    return;
                }

                res.once('error', function onLsError(err) {
                    cb(xlateMantaError(ls_err));
                });

                res.on('entry', function onLsEntry(e) {
                    names.push(e.name);
                    out.write(JSON.stringify(e) + '\n');
                });

                res.once('end', function onLsDone() {
                    out.once('finish', function onFileWritten() {
                        var key = sprintf(FILES_KEY_FMT, _path);
                        self.db.get(key, function onDbGet(db_err, info) {
                            if (db_err) {
                                cb(xlateDBError(db_err));
                                return;
                            }

                            info._cacheFile = cacheFile;
                            stats._cacheFile = cacheFile;
                            self.db.put(key, info, function onDbWrite(err) {
                                if (err) {
                                    cb(xlateDBError(err));
                                } else {
                                    self.cache.set(key, stats);
                                    cb(null, names);
                                }
                            });
                        });
                    });
                    out.end();
                });
            });
        });
    }, true);
};


/**
 * Implementation of fs.stat() [really stat(2)].
 *
 * Arguments are standard, except the "internal" is a secret flag
 * internal functions can use to skip the `nextTick` tax upstack
 * callers would need to hit when our info is already cached.
 *
 */
MantaFs.prototype.stat = function stat(_path, cb, internal) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');
    assert.optionalBool(internal, 'internal');

    cb = once(cb);
    if (!internal)
        _path = path.normalize(_path);

    var key = sprintf(FILES_KEY_FMT, _path);
    var stats;

    if ((stats = this.cache.get(key))) {
        if (internal) {
            cb(null, stats);
        } else {
            setImmediate(cb.bind(this, null, stats));
        }
    } else {
        this._stat(_path, cb);
    }
};


MantaFs.prototype.toString = function toString() {
    var FMT =
        '[object %s<root=%s, manta=%s, max_files=%d, max_size=%d, ' +
        'ttl=%d>]';
    return (sprintf(FMT,
                    this.constructor.name,
                    this.path,
                    this.manta.toString(),
                    this.max_files,
                    this.max_size,
                    (this.ttl / 1000)));
};


//-- Private Methods

MantaFs.prototype._evict = function _evict(key) {
    // TODO
};


/**
 * Ensures that we have a filename-to-filehandle mapping
 *
 * If the mapping already exists, we're done, otheriwse we create
 * and record a UUID format.
 *
 * Errors returned from this method do not need to be translated.
 *
 */
MantaFs.prototype._fhandle = function _fhandle(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var fhandle;
    var k = sprintf(FHANDLE_KEY_FMT, _path);
    var self = this;

    this.db.get(k, function (err, val) {
        if (!err && val) {
            cb(null, val);
            return;
        }

        fhandle = libuuid.create();
        var k2 = sprintf(FNAME_KEY_FMT, fhandle);

        self.db.batch()
            .put(k, fhandle)
            .put(k2, _path)
            .write(function (err2) {
                if (err2) {
                    cb(xlateDBError(err2));
                } else {
                    cb(null, fhandle);
                }
            });
    });
};


/**
 * This is a private setter for stat(2).
 *
 * _stat gets invoked when we have a cache miss for a stat
 * call. This method will:
 *
 * - Ensure the file path has an `fhandle` mapping (and vice versa)
 * - Perform a Manta HEAD on the specified path
 * - Stash ^^ in leveldb and in-memory LRUs
 *
 * Errors returned from this do not need to be translated.
 *
 */
MantaFs.prototype._stat = function _stat(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    var self = this;

    this._fhandle(_path, function onFhandle(f_err, fhandle) {
        if (f_err) {
            cb(f_err);
            return;
        }

        self.manta.info(_path, function onInfo(m_err, info) {
            if (m_err) {
                cb(xlateMantaError(m_err));
                return;
            }

            var key = sprintf(FILES_KEY_FMT, _path);
            var stats = mantaToStats(_path, info);
            stats._fhandle = fhandle;

            self.db.put(key, info, function onDbWriteDone(err) {
                if (err) {
                    cb(xlateDBError(err));
                    return;
                }

                self.cache.set(key, stats);
                cb(null, self.cache.get(key));
            });
        });
    });
};



///--- Exports

module.exports = {
    MantaFs: MantaFs,

    createClient: function createClient(opts) {
        assert.object(opts, 'options');

        var _opts = {
            files: opts.files,
            log: opts.log || bunyan.createLogger({
                stream: process.stderr,
                level: 'warn',
                name: 'MantaFs',
                serializers: bunyan.stdSerializers
            }),
            path: opts.path,
            manta: opts.manta,
            sizeMB: opts.sizeMB,
            ttl: opts.ttl || 3600
        };

        return (new MantaFs(_opts));
    }
};
