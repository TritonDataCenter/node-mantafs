// Copyright 2014 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var events = require('events');
var fs = require('fs');
var path = require('path');
var util = require('util');
var vasync = require('vasync');

var assert = require('assert-plus');
var levelup = require('levelup');
var LRU = require('lru-cache');
var LineStream = require('lstream');
var manta = require('manta');
var once = require('once');
var uuid = require('node-uuid');

var DirStream = require('./readdir_stream');
var ErrnoError = require('./errors').ErrnoError;
var xlateMantaError = require('./errors').xlateMantaError;
var utils = require('./utils');


///--- Globals

var sprintf = util.format;

var CACHE_DIR_NAME = 'fscache';
var DB_NAME = 'mantafs.db';

// DB key for tracking dirty files (pathname to timestamp)
var DIRTY_KEY_FMT = ':d:%s';
// DB key for pathname to uuid mapping
var FHANDLE_KEY_FMT = ':fh:%s';
// DB key for pathname to 'stats' data (see mantaToStats)
var FILES_KEY_FMT = ':f:%s';
// DB key for uuid to pathname mapping
var FNAME_KEY_FMT = ':fn:%s';

// The data we track for writeback is used to ensure that we never miss
// writing a file back to Manta. There are several tricky considerations here.
//
// The 'age' configuration value is used to indicate how long a file can
// be dirty in the cache before we schedule a writeback. 'age' is not a
// quarantee as to when the writeback will occur since other writeback activity
// can delay when the file is actually written back.
//
// We need to ensure that a file which is being written to on a regular basis
// is not starved from ever being written back. We need to ensure that if we
// are restarted while the file is dirty, that the file gets scheduled for
// writeback. We also don't want to writeback the file more times than is
// necessary (i.e. writeback should coalesce as much write activity as
// possible) but we need to handle the case where the file is updated while
// in the middle of a writeback.
//
// Writeback tracking is managed as follows:
// 1) Every time we write to the file, we update the file's dirty timestamp
//    in the DB. In this way, if we restart, we will schedule a writeback
//    for the file in, at most, 'age' after the restart. If the server was
//    down for longer than 'age', then the writeback can be scheduled
//    immediately.
// 2) We use an in-memory list (wb_pending) to also track writeback scheduling.
//    The first time we write to a file, we add the file to the list (using the
//    current time value). On subsequent writes, if the file is already in the
//    list, we don't make any change. This ensures that the file's writeback is
//    not starved by subsequent writes.
// 3) The writeback scheduling uses the in-memory list to determine which files
//    to writeback. When the writeback is running, it processes this list and
//    writes back each file that has been on the list for at least 'age'. At
//    the start of writing back a file, the file is removed from the in-memory
//    list. This ensures that any subsequent writes to the file are rescheduled
//    for a future writeback (since we don't have any way to tell if the write
//    was to a portion of the file which was already uploaded). While the file
//    is being written back, it is stored on the in-memory wb_inprogress list.
//    This list is used to control how file removal and truncation is managed
//    while a file is being uploaded.
// 4) When the file has completed writing back, we check the in-memory list
//    to see if the file was modified while we were writing back (i.e. if an
//    entry exists for the file, since we removed the entry when we started
//    writing back). If not, then we delete the DB record for the file since
//    it is no longer dirty.

// writeback is enabled when we have dirty files
var writeback_enabled = false;
// our in-memory list of dirty files
var wb_pending = {};
// our in-memory list of files that are currently being written back
var wb_inprogress = {};
// some context for the _writeback function; holds the 'age', 'db', 'location',
// 'log', 'manta' and 'num_par' references
var wb_context = {};
// the number of concurrently running writebacks
var wb_num = 0;

///--- Helpers

function bytes(mb) {
    assert.number(mb, 'megabytes');

    return (Math.floor(mb * 1024  *1024));
}


// We know that all values going into put/get are the results of
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


// This function drives the writeback of dirty files
// Its initiated on startup if we have dirty files in the DB and its scheduled
// by the 'dirty' function if necessary.
// It also calls itself when a writeback has completed to see if there is more
// work to do.
function _writeback() {
    var now = new Date().getTime();
    var threshold = now - wb_context.age;

    function _doUpload(_arg, _cb) {
        var cachefile = path.join(wb_context.location, _arg.cachefile);
        wb_context.log.debug('writeback %s => %s', _arg.cachefile, _arg.fname);

        fs.stat(cachefile, function onStatDone(s_err, stats) {
            if (s_err) {
                // This should never happen
                wb_context.log.error(s_err,
                    '_doUpload(%s): error stat-ing local file (%s)',
                    _arg.fname, cachefile);
                _cb(s_err, _arg);
            }

            // Only read the current size in case the file gets extended
            // while we're uploading. In that case it will be dirty again
            // and be re-uploaded.
            var rd_size = stats.size;
            if (rd_size > 0)
                rd_size -= 1;
            var r_opts = {
                start: 0,
                end: rd_size
            };
            var m_opts = {
                size: stats.size
            };
            var rstream = fs.createReadStream(cachefile, r_opts);
            rstream.once('error', function onFileError(err) {
                wb_context.log.error(err,
                    '_doUpload(%s): error reading from local file (%s)',
                    _arg.fname, cachefile);
                _cb(err, _arg);
            });

            rstream.once('open', function onOpen() {
                wb_context.manta.put(_arg.fname, rstream, m_opts,
                  function (err) {
                    if (err) {
                        wb_context.log.error(err,
                            '_doUpload(%s): error pushing (%s) to manta:',
                                _arg.fname, cachefile);
                        _cb(err, _arg);
                    } else {
                        wb_context.log.debug('_doUpload(%s) pushed to (%s):',
                            cachefile, _arg.fname);
                        _cb(null, _arg);
                    }
                });
            });
        });
    }

    function _fileUploaded(_arg, _cb) {
        if (!wb_pending[_arg.fname]) {
            // if no more writes while we were uploading, we remove it from
            // the DB since its no longer dirty
            var key = sprintf(DIRTY_KEY_FMT, _arg.fname);
            wb_context.db.batch()
                .del(key)
                .write(function onBatchDel(err) {
                    if (err) {
                        wb_context.log.warn(err, 'dirty del(%s): failed',
                            _arg.fname);
                        _cb(errno(err, _arg));
                    } else {
                        wb_context.log.trace('dirty del(%s): done', _arg.fname);
                        _cb(null, _arg);
                    }
                });
        }
    }

    function _wb_done(err, res) {
        // just use the return from the first operation to get the args
        var args = res.operations[0].result;
        if (err) {
            wb_context.log.error(err, 'writeback: error %s', args.fname);
            // XXX JJ generalize error handling
            // e.g. if (err.code === 'UploadTimeout') ...
            // other manta errors, etc.
            // On upload timeout we won't have called the _fileUploaded
            // function in the pipeline, so we still have the file in the DB,
            // but not the wb_pending list. We'll add this entry back into the
            // in-memory wb_pending list (or overwrite the existing entry if
            // the file was updated while we tried to upload), but with a
            // timestamp one minute in the future, so that we'll retry the
            // upload of this file a little later. We still keep trying to
            // upload any other files in the list. This handles the case in
            // which the failure was somehow relevant only to this file. If all
            // uploads fail, we'll have just added them back to the writeback
            // list for future retries.
            var plusminute = new Date().getTime();
            plusminute += 60000;
            wb_pending[args.fname] = {
                cachefile: args.cachefile,
                timestamp: plusminute
            };
        } else {
            wb_context.log.debug('writeback %s done', args.fname);
        }
        // remove it from the in-memory inprogress list
        delete wb_inprogress[args.fname];
        wb_num--;

        // take another lap
        _writeback();
    }

    // When we iterate using 'in', the elements in wb_pending are probably
    // ordered by the order in which they were defined. On startup, when
    // pulling out of the DB, we define them in the order we get them from the
    // DB which, since its a btree, will be alphabetical. Thus, we have to look
    // at each entry to see if its time to write it back. That is, the entries
    // might not be in timestamp order so we can't stop when we see an entry
    // that's not ready to be written back.
    //
    // Once we start processing the list, we'll get called back and keep making
    // a pass over the list until we have made a full pass with no writeback.
    // At that time, if there are still elements in the list, we schedule
    // ourselves to wakeup later and process the list again.

    var wb_running = false; 
    var more_files = false; 
    var fname;
    for (fname in wb_pending) {
        if (threshold > wb_pending[fname].timestamp) {
            var file_data = {
                fname: fname,
                cachefile: wb_pending[fname].cachefile
            };

            // remove it from the in-memory list
            delete wb_pending[fname];

            // add it to the in-memory in progress list
            wb_inprogress[fname] = true;
            wb_num++;
            wb_running = true; 

            // here is where we setup the writeback pipeline
            vasync.pipeline({
                funcs: [
                    _doUpload,
                    _fileUploaded
                ],
                arg: file_data
            }, _wb_done);

            if (wb_num >= wb_context.num_par) {
                // we've hit our limit for parallel writebacks, break out of
                // the loop since we'll callback to _writeback when one of the
                // uploads has finished
                break;
            }
        } else {
            more_files = true;
        }
    }

    if (!wb_running) {
        if (more_files) {
            setTimeout(_writeback, wb_context.age);
        } else {
            writeback_enabled = false;
        }
    }
}


///--- API

function Cache(opts) {
    assert.object(opts, 'options');
    // assert.number(opts.dirty, 'options.dirty');
    assert.object(opts.log, 'options.log');
    assert.string(opts.location, 'options.location');
    assert.number(opts.size, 'options.size');
    assert.number(opts.ttl, 'options.ttl');
    assert.number(opts.wbtime, 'options.wbtime');
    assert.number(opts.num_par, 'options.num_par');

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
    this.wbtime = opts.wbtime;
    this.num_par = opts.num_par;
    wb_context.age = this.wbtime;
    wb_context.log = this.log;
    wb_context.location = path.join(this.location, 'fscache');
    wb_context.manta = opts.manta;
    wb_context.num_par = this.num_par;

    this._mantafs_cache = true; // MDB flag

    // When we first startup we'll process any dirty files that might be left
    // in our cache during the open() function.
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
            fs.stat(info._cacheFile, function onStatDone(err2, stats) {
                if (err2) {
                    if (err2.code !== 'ENOENT') {
                        log.trace(err2, 'dataInCache(%s): failed', p);
                        cb(err2);
                        return;
                    }
                    if (info.size === 0) {
                        fs.writeFile(p, new Buffer(0), function (err3) {
                            if (err) {
                                log.trace(err3,
                                          'dataInCache(%s): failed (truncate)',
                                          p);
                                cb(err);
                            } else {
                                log.trace('dataInCache(%s, %d): %j', p, pos,
                                          true);
                                cb(null, true, info);
                            }
                        });
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


/**
 * Mark a file as being dirty in the cache and needing to be written back
 * to Manta.
 *
 * `path` is the local cache file, not the manta path.
 */
Cache.prototype.dirty = function dirty(p, cachefile, cb) {
    assert.string(p, 'path');
    assert.string(cachefile, 'cachefile');
    assert.func(cb, 'callback');

    cb = once(cb);

    var self = this;
    var log = this.log;
    var k = sprintf(DIRTY_KEY_FMT, p);

    var now = new Date().getTime();
    var wb_data = {
        cachefile: cachefile,
        timestamp: now
    };
    if (!wb_pending[p]) {
        // first time being dirtied, add to in-memory lists
        wb_pending[p] = wb_data;
    }
    // make sure that writeback is enabled
    if (!writeback_enabled) {
        writeback_enabled = true;
        // schedule for 5ms after the timeout to be sure we writeback this
        // initial file in the list.
        setTimeout(_writeback, wb_context.age + 5);
    }
    self.db.batch()
        .put(k, wb_data)
        .write(function onBatchWrite(err) {
            if (err) {
                log.warn(err, 'dirty put(%s): failed', p);
                cb(errno(err));
            } else {
                var changed = new Date(wb_data.timestamp);
                log.trace('dirty(%s) done => %s', p, changed.toUTCString());
                cb(null);
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


Cache.prototype.put = function put(p, stats, cb) {
    assert.string(p, 'path');
    assert.object(stats, 'stats');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('put(%s, %j): entered', p, stats);

    this.get(p, function onGet(err, val) {
        if (err) {
            cb(err);
            return;
        } if (val) {
            stats._fhandle = val._fhandle;
            stats._cacheFile = val._cacheFile;
        } else {
            stats._fhandle = uuid.v4();
            stats._cacheFile =
                path.join(self.location, 'fscache', stats._fhandle);
        }

        p = manta.path(p);
        var key = sprintf(FILES_KEY_FMT, p);
        var k1 = sprintf(FHANDLE_KEY_FMT, p);
        var k2 = sprintf(FNAME_KEY_FMT, stats._fhandle);

        self.db.batch()
            .put(key, stats)
            .put(k1, stats._fhandle)
            .put(k2, p)
            .write(function onBatchWrite(err2) {
                if (err2) {
                    log.trace(err2, 'put(%s): failed', p);
                    cb(errno(err2));
                } else {
                    log.trace('put(%s): done', p);
                    self.cache.set(key, stats);
                    cb(null, stats);
                }
            });
    });
};


// Removing a file from the cache is complicated by the interaction with
// writeback. If the file is pending writeback, we can cleanup the bookkeeping
// immediately and the file will never get written back. However, if the
// file is in the middle of being written back, we cannot remove the file
// until that operation has completed.
Cache.prototype.del = function del(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    function cleanup_cache(_fhandle, _cb) {
        var key = sprintf(FILES_KEY_FMT, p);
        var fhk = sprintf(FHANDLE_KEY_FMT, p);
        var fnk = sprintf(FNAME_KEY_FMT, _fhandle);
        var dtk = sprintf(DIRTY_KEY_FMT, p);
        self.db.batch()
            .del(key)
            .del(fhk)
            .del(fnk)
            .del(dtk)
            .write(function onBatchDel(err2) {
                if (err2) {
                    log.error(err2, 'del(%s): failed', p);
                    _cb(errno(err2));
                } else {
                    // remove from LRU cache
                    self.cache.del(key);

                    var cachefile = path.join(wb_context.location, _fhandle);
                    fs.unlink(cachefile, function (u_err) {
                        // ignore errors deleting the cachefile

                        // cleaned up locally, now remove from manta
                        wb_context.manta.unlink(p, function (m_err) {
                            if (m_err) {
                                // A NotFoundError is ok since that means the
                                // file doesn't exist in manta. This can happen
                                // if we create and delete a file before we
                                // ever write it back.
                                if (m_err.name === 'ResourceNotFoundError') {
                                    log.trace('del(%s): done', p);
                                    _cb(null);
                                } else {
                                    log.error(m_err, 'del(%s): failed', p);
                                    _cb(m_err);
                                }
                            } else {
                                log.trace('del(%s): done', p);
                                _cb(null);
                            }
                        });
                    });
                }
            });
    }

    p = manta.path(p);
    log.trace('del(%s): entered', p);
    if (!self.has(p)) {
        // no data for this file is cached, just delete it in manta
        wb_context.manta.unlink(p, function (m_err) {
            if (m_err) {
                // A NotFoundError is ok since that means the file doesn't
                // exist in manta. This can happen if we create and delete a
                // file before we ever write it back.
                if (m_err.name === 'ResourceNotFoundError') {
                    log.trace('del(%s): done', p);
                    cb(null);
                } else {
                    log.error(m_err, 'del(%s): failed', p);
                    cb(m_err);
                }
            } else {
                log.trace('del(%s): done', p);
                cb(null);
            }
        });
    } else {
        this.get(p, function (err, info) {
            if (err) {
                log.error(err, 'del(%s): failed', p);
                cb(err);
                return;
            }

            if (info.extension === 'directory') {
                // We can do a directory immediately since no writeback
                cleanup_cache(info._fhandle, cb);
            } else {
                // For a plain file we have to manage the interaction with
                // writeback.
                if (wb_inprogress[p]) {
                    // XXX JJ we can't delete the file while it is being written
                    // back
                }

                if (wb_pending[p]) {
                    // remove it from the writeback in-memory list
                    delete wb_pending[p];
                }

                cleanup_cache(info._fhandle, cb);
            }
        });
    }
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
    assert.string(p, 'path');
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
        self.dataInCache(p, pos, function onIsReady(err, ready) {
            if (err) {
                log.trace(err, 'wait(%s, %d, %d): failed', p, info.size, pos);
                cb(err);
            } else if (!ready) {
                setTimeout(poll, 10);
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
        wb_context.db = self.db;

        var enable_wb = false;

        self.db.on('error', self.emit.bind(self, 'error'));
        self.db.once('ready', function onDatabase() {
            /* JSSTYLED */
            var d_re = /^:d:/;
            var keys = self.db.createReadStream();
            /* JSSTYLED */
            var f_re = /^:f:/;

            // XXX JJ
            // We happen to know (since it's a BTree) that we're going
            // to see the keys for "dirty" before "files", so if it's dirty,
            // we just keep a table of them, and overwrite the files as we
            // see them. Janky, but it works.
            keys.on('data', function onDbEntry(data) {
                log.trace('open: DB entry: %j', data);
                if (d_re.test(data.key)) {
                    // add this cached file to the writeback work queue
                    var k = data.key.split(':').pop();
                    enable_wb = true;
                    wb_pending[k] = data.value;

                    // TODO - add to dirty queue
                    // self._timers[data.key] =
                    //     setTimeout(self._upload.bind(self, data.key),
                    //                self.dirtyAge);
                } else if (f_re.test(data.key)) {
// XXX JJ fix this up or remove
//                  var k = data.key.split(':').pop();
//                  var v = data.value;
//
//                  self.size += v.size;
//                  self.cache.set(data.key, v);
                }
            });
            keys.once('error', self.emit.bind(self, 'error'));
            keys.once('end', function () {
                log.debug('open: done');
                if (enable_wb) {
                    // process the backlog of writebacks
                    writeback_enabled = true;
                    setTimeout(_writeback, 100);
                }
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
        'max_size=' + this.max_size + ', ' +
        'ttl=' + this.ttl + ', ' +
        'wbtime=' + this.wbtime + ', ' +
        'num_par=' + this.num_par + ', ' +
        'current_size=' + this.current_size + '>]';

    return (str);
};
