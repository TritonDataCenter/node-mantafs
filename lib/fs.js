// Copyright 2014 Joyent, Inc.  All rights reserved.
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
var LineStream = require('lstream');
var manta = require('manta');
var once = require('once');
var uuid = require('node-uuid');
var vasync = require('vasync');

var cache = require('./cache');
var errors = require('./errors');
var ReadStream = require('./readable').ReadStream;
var WriteStream = require('./writable').WriteStream;



///--- Globals

var sprintf = util.format;
var ErrnoError = errors.ErrnoError;
var xlateDBError = errors.xlateDBError;
var xlateMantaError = errors.xlateMantaError;

var FHANDLE_KEY_FMT = '::fhandles:%s';
var FNAME_KEY_FMT = '::fnames:%s';
var FILES_KEY_FMT = '::files:%s';
var DIRTY_KEY_FMT = '::dirty:%s';



///--- Helpers

// Increments our fd - we start at 3
function incr(i) {
    if (++i >= Math.pow(2, 32) - 1)
        i = 3;
    return (i);
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
    // XXX (broken, same for all files) stats.ino = crc32.calculate(info.etag);
    stats.ino = crc32.calculate(_path);
    if (info.extension === 'directory') {
        stats.nlink = parseInt(info.headers['result-set-size'], 10);
        stats.isFile = _false;
        stats.isDirectory = _true;
        stats.mode = 0755;
        stats.size = 0; // XXX makeup something else?
    } else {
        stats.nlink = 1;
        stats.isFile = _true;
        stats.isDirectory = _false;
        stats.mode = 0644;
        stats.size = info.size;
    }
    stats.uid = -2;    // TODO: this is Mac-only
    stats.gid = -2;    // TODO: this is Mac-only
    stats.rdev = 0;
    stats.atime = new Date();
    stats.mtime = new Date(info.headers['last-modified']);
    stats.ctime = stats.mtime;

    stats.isBlockDevice = _false;
    stats.isCharacterDevice = _false;
    stats.isSymbolicLink = _false;
    stats.isFIFO = _false;
    stats.isSocket = _false;

    stats._cacheFile = stats._cacheFile || info._cacheFile;
    stats._fds = [];
    stats._fhandle = stats._fhandle || info._fhandle;
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
 * - log<Bunyan>: log handle
 *
 * Once you instantiate this, wait for the `ready` event.
 */
function MantaFs(opts) {
    assert.object(opts, 'options');
    assert.object(opts.cache, 'options.cache');
    assert.object(opts.log, 'options.log');
    assert.object(opts.manta, 'options.manta');

    events.EventEmitter.call(this, opts);

    this.cache = opts.cache;

    this.log = opts.log.child({component: 'MantaFsApi'}, true);
    this.manta = opts.manta;

    this._fd = 3;
    this._fds = {};

    // LRU eviction is asynchronous, and sometimes triggered when
    // we don't want it (like in readdir/stat). So we have to do
    // some book-keeping so that evictions/uploads skip entries we know
    // are "hot"
    this._no_evict = {};
    this._no_upload = {};
    this._timers = {};

    this.cache.on('close', this.emit.bind(this, 'close'));
    this.cache.on('error', this.emit.bind(this, 'error'));
    this.cache.once('ready', this.emit.bind(this, 'ready'));
}
util.inherits(MantaFs, events.EventEmitter);
module.exports = MantaFs;


/**
 * Closes an `fd` that was obtained (and used) via `open`.
 */
MantaFs.prototype.close = function close(fd, cb) {
    assert.number(fd, 'fd');
    assert.func(cb, 'callback');

    var log = this.log;

    if (!(this._fds[fd])) {
        setImmediate(cb.bind(this, new ErrnoError('EBADF', 'close')));
    } else {
        log.trace('close(%d): entered', fd);
        delete this._fds[fd];
        setImmediate(function () {
            log.trace('close(%d): done', fd);
            cb();
        });
    }
};


/**
 * Returns a new ReadStream object (See Readable Stream).
 *
 * options is an object with the following defaults:
 *
 * {
 *   flags: 'r',
 *   encoding: null,
 *   fd: null,
 *   mode: 0666,
 *   autoClose: true
 * }
 *
 * options can include start and end values to read a range of bytes from the
 * file instead of the entire file. Both start and end are inclusive and start
 * at 0. The encoding can be 'utf8', 'ascii', or 'base64'.
 *
 * If autoClose is false, then the file descriptor won't be closed, even if
 * there's an error. It is your responsiblity to close it and make sure there's
 * no file descriptor leak. If autoClose is set to true (default behavior), on
 * error or end the file descriptor will be closed automatically.
 *
 * An example to read the last 10 bytes of a file which is 100 bytes long:
 *
 * fs.createReadStream('sample.txt', {start: 90, end: 99});
 *
MantaFs.prototype.createReadStream = function createReadStream(_path, opts) {
    assert.string(_path);
    assert.optionalObject(opts);
    opts = opts || {};
    assert.optionalString(opts.flags, 'options.flags');
    if (opts.encoding !== null)
        assert.optionalString(opts.encoding, 'options.encoding');
    assert.optionalNumber(opts.fd, 'options.fd');
    assert.optionalNumber(opts.mode, 'options.mode');
    assert.optionalBool(opts.autoClose, 'options.autoClose');

    var _opts = {
        autoClose: opts.autoClose,
        encoding: opts.encoding,
        fs: this,
        start: opts.start,
        end: opts.end
    };
    var rstream;

    if (opts.fd) {
        _opts.fd = opts.fd;
        rstream = new ReadStream(_opts);
    } else {
        rstream = new ReadStream(_opts);
        this.open(_path,
                  (opts.flags || 'r'),
                  (opts.mode || 0666),
                  function onOpen(err, fd) {
                      if (err) {
                          rstream.emit('error', err);
                      } else {
                          rstream._open(fd);
                      }
                  });
    }

    return (rstream);
};
*/


/**
 * Returns a new WriteStream object (See Writable Stream).
 *
 * options is an object with the following defaults:
 *
 * {
 *   flags: 'w',
 *   encoding: null,
 *   mode: 0666
 * }
 *
 * options may also include a start option to allow writing data at some
 * position past the beginning of the file. Modifying a file rather than
 * replacing it may require a flags mode of r+ rather than the default mode w.
 *
 *
MantaFs.prototype.createWriteStream = function createWriteStream(_path, opts) {
    assert.string(_path);
    assert.optionalObject(opts);
    opts = opts || {};
    assert.optionalString(opts.flags, 'options.flags');
    if (opts.encoding !== null)
        assert.optionalString(opts.encoding, 'options.encoding');
    assert.optionalNumber(opts.mode, 'options.mode');

    var _opts = {
        encoding: opts.encoding || null,
        flags: opts.flags || 'w',
        fs: this,
        mode: opts.mode || 0666,
        start: opts.start
    };
    var wstream = new WriteStream(_opts);

    this.open(_path, _opts.flags, _opts.mode, function onOpen(err, fd) {
        if (err) {
            wstream.emit('error', err);
        } else {
            wstream._open(fd);
        }
    });

    return (wstream);
};
*/

/**
 * Bonus operation that translates an opaque file handle back to
 * the original path (useful for NFS).
 */
MantaFs.prototype.fhandle = function _fhandle(fh, cb) {
    assert.string(fh, 'fhandle');
    assert.func(cb, 'callback');

    this.cache.fhandle(fh, cb);
};


/**
 * Pushes a file into manta synchronously;  this is expensive, so be sure
 * that you want to do this.
 */
MantaFs.prototype.fsync = function fsync(fd, cb) {
    assert.number(fd, 'fd');
    assert.func(cb, 'callback');

    if (!(this._fds[fd]) || !this._fds[fd].stats) {
        setImmediate(cb.bind(this, new ErrnoError('EBADF', 'fsync')));
    } else if (!this._fds[fd].stats) {
        setImmediate(cb.bind(this));
    } else {
        this._upload(sprintf(DIRTY_KEY_FMT, this._fds[fd].stats._path), cb);
    }
};


MantaFs.prototype.ftruncate = function ftruncate(fd, len, cb) {
    assert.number(fd, 'fd');
    assert.number(len, 'len');
    assert.func(cb, 'callback');

    var map;

    if (!(map = this._fds[fd])) {
        setImmediate(function invalidFD() {
            cb(new ErrnoError('EBADF', 'truncate'));
        });
        return;
    }

    this.truncate(map.stats._path, len, cb);
};


/**
 * lookup is a "bonus" operation required by NFS. It returns an "opaque handle"
 * of string type given some path name. Since MantaFS already has to keep a
 * of mapping of full path filename to uuid, we just use that.
 *
 * Callback is of the form `function (err, fhandle) {}`, where `fhandle` is a
 * string (uuid).
 */
MantaFs.prototype.lookup = function lookup(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    this.cache.lookup(manta.path(p, true), cb);
};


/**
 * In the `mkdir` operation, we just go ahead and always call the
 * Manta API, as this is cheap, and saves us conflicts down the road.
 *
 */
MantaFs.prototype.mkdir = function mkdir(p, mode, cb) {
    assert.string(p, 'path');
    if (typeof (mode) === 'function') {
        cb = mode;
        mode = 0777;
    }
    assert.number(mode, 'mode');
    assert.func(cb, 'callback');

    p = manta.path(p, true);
    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('mkdir(%s, %d): entered', p, mode);
    this._ensureParent(p, function onParent(p_err) {
        if (p_err) {
            log.trace(p_err, 'mkdir(%s): failed', p);
            cb(p_err);
            return;
        }

        self.manta.mkdir(p, function onMkdir(m_err, info) {
            if (m_err) {
                log.trace(m_err, 'mkdir(%s): failed', p);
                cb(m_err);
                return;
            }

            self._stat(p, info, function onStat(s_err) {
                if (s_err) {
                    log.trace(s_err, 'mkdir(%s): failed', p);
                    cb(s_err);
                    return;
                }

                self.cache.dirtyParent(p, function (err) {
                    if (err) {
                        log.trace(err, 'mkdir(%s): failed', p);
                        cb(err);
                    } else {
                        log.trace('mkdir(%s): done', p);
                        cb();
                    }
                });
            });
        });
    });
};


/**
 * Implementation of fs.open()
 *
 * This will stat a file, and if it does not exist in the cache,
 * automatically start streaming it down.
 */
MantaFs.prototype.open = function open(p, flags, mode, cb) {
    assert.string(p, 'path');
    assert.string(flags, 'flags');
    if (typeof (mode) === 'function') {
        cb = mode;
        mode = 0666;
    }
    assert.number(mode, 'mode');
    assert.func(cb, 'callback');

    p = manta.path(p, true);
    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('open(%s, %s, %d): entered', p, flags, mode);

    function _checkForDownload(_arg, _cb) {
        if (self.cache.inFlight(p) || _arg.create) {
            _cb();
            return;
        }

        self._get_manta_info(p, function (s_err, info) {
            if (s_err) {
                if (s_err.code === 'ENOENT' && _arg.create) {
                    _arg.run_create = true;
                    _cb();
                } else {
                    _cb(s_err);
                }
            } else {
                _arg.info = info;
                fs.stat(info._cacheFile, function (err, stats) {
                    if (err) {
                        if (err.code === 'ENOENT') {
                            _arg.download = true;
                            _cb();
                        } else {
                            _cb(err);
                        }
                    } else {
                        _cb();
                    }
                });
            }
        });
    }

    // When creating use _stat to save the zero-size new file info into the
    // cache and create an empty local file.
    function _zero(_arg, _cb) {
        if (!_arg.create) {
            _cb();
            return;
        }

        // We need to stub this out now since we won't write the file to manta
        // until writeback runs on this file.
        var now = new Date().toUTCString();
        var info = {
            extension: 'bin',
            type: 'application/octet-stream',
            parent: path.dirname(p),
            size: 0,
            headers: {
                'last-modified': now
            }
        };

        self._stat(p, info, function (s_err, stats) {
            if (s_err) {
                _cb(s_err);
                return;
            }

            fs.writeFile(stats._cacheFile, new Buffer(0), function (err) {
                if (err) {
                    _cb(err);
                    return;
                }

                // mark file as dirty in the cache
                self.cache.dirty(p, stats._fhandle, function (d_err) {
                    if (d_err) {
                        log.warn(d_err, 'dirty(%s): failed', p);
                        _cb(d_err);
                    } else {
                        self.cache.dirtyParent(p, _cb);
                        // var _assign_fd = assign_fd.bind(self, stats);
                        // self._stageParent(_path, info, pstats, _assign_fd);
                    }
                });
            });
        });
    }

    function _startDownload(_arg, _cb) {
        if (!_arg.download || _arg.create) {
            _cb();
            return;
        }

        var _opts = {
            path: _arg.info._cacheFile
        };
        self.cache.inFlight(p, true);
        self.manta.cacheObject(p, _opts, function () {
            self.cache.inFlight(p, false);
        });
        setImmediate(_cb);
    }

    function _ensureCacheFile(_opts) {
        var _arg = _opts || {};
        vasync.pipeline({
            funcs: [
                _checkForDownload,
                _zero,
                _startDownload
            ],
            arg: _arg
        }, function (err) {
            if (err) {
                log.trace(err, 'open(%s, %s, %d): failed', p, flags, mode);
                cb(err);
                return;
            }

            self._fd = incr(self._fd);
            self._fds[self._fd] = {
                flags: flags,
                pos: /a/.test(flags) ? _arg.info.size : 0,
                info: _arg.info,
                path: p
            };
            log.trace('open(%s, %s, %d): done => %d', p, flags, mode, self._fd);
            cb(null, self._fd);
        });
    }

    switch (flags) {
    case 'a':
    case 'a+':
    case 'r':
    case 'r+':
    case 'rs':
    case 'rs+':
        _ensureCacheFile();
        break;

    case 'w':
    case 'w+':
        _ensureCacheFile({
            create: true
        });
        break;

    default:
        cb(new ErrnoError('EINVAL', 'open'));
        break;
    }
};


    // function assign_fd(stats, err) {
    //     if (err) {
    //         log.trace(err, 'open(%s): failed', p);
    //         cb(err);
    //     } else {
    //         self._fd = incr(self._fd);
    //         self._fds[self._fd] = {
    //             flags: flags,
    //             pos: /a/.test(flags) ? info.size : 0,
    //             info: info
    //         };
    //         cb(null, self._fd);
    //     }
    // }



    // this._stat(p, function onStat(s_err, info) {
    //     if (s_err) {
    //         if (s_err.code === 'ENOENT' && /w/.test(flags)) {
    //             log.trace('open(%s): need to create file', p);
    //             creat();
    //         } else {
    //             log.trace(s_err, 'open(%s): stat error', p);
    //             cb(s_err);
    //         }
    //         return;
    //     }

    //     var _assign_fd = assign_fd.bind(self, info);
    //     // Anything with 's' is a LIE!
    //     switch (flags) {
    //     case 'a':
    //     case 'a+':
    //     case 'r':
    //     case 'r+':
    //     case 'rs':
    //     case 'rs+':
    //         if (self.cache)
    //         var sz = info.size ? 1 : 0;
    //         self.manta.wait(stats._cacheFile, info, sz, _assign_fd);
    //         break;

    //     case 'w':
    //     case 'w+':
    //         self.truncate(_path, 0, _assign_fd);
    //         break;

    //     default:
    //         cb(new ErrnoError('EINVAL', 'open'));
    //         break;
    //     }
    // }, true);



/**
 * Read data from the file specified by fd.
 *
 * buffer is the buffer that the data will be written to.
 *
 * offset is the offset in the buffer to start writing at.
 *
 * length is an integer specifying the number of bytes to read.
 *
 * position is an integer specifying where to begin reading from in the file.
 * If position is null, data will be read from the current file position.
 *
 * The callback is given the three arguments, (err, bytesRead, buffer).
 */
MantaFs.prototype.read = function read(fd, buf, off, len, pos, cb) {
    assert.number(fd, 'fd');
    assert.buffer(buf, 'buffer');
    assert.number(off, 'offset');
    assert.number(len, 'length');
    if (typeof (pos) === 'function') {
        cb = pos;
        pos = null;
    }
    if (pos !== null)
        assert.number(pos, 'position');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    if (!this._fds[fd]) {
        setImmediate(function invalidFD() {
            cb(new ErrnoError('EBADF', 'read'));
        });
        return;
    }

    var fmap = this._fds[fd];
    var fname = fmap.info._cacheFile;
    var _pos = (pos !== null ? pos : fmap.pos);

    log.trace('read(%d, %d, %d): entered', fd, len, pos);

    this.cache.dataInCache(fmap.path, _pos + len,
      function (c_err, ready, info) {
        if (c_err) {
            log.trace(c_err, 'read(%d, %d, %d): failed', fd, len, _pos);
            cb(c_err);
            return;
        }

        if (len + _pos > info.size)
            len = info.size - _pos;

        function onReadDone(err, nbytes) {
            if (err) {
                log.trace(err, 'read(%d, %d, %d): failed', fd, len, _pos);
                cb(err);
            } else {
                if (pos === null)
                    fmap.pos += nbytes;
                log.trace(err, 'read(%d, %d, %d): done => %d',
                          fd, len, _pos, nbytes);
                cb(null, nbytes, buf);
            }
        }

        if (ready) {
            log.trace('read(%d, %d, %d): data is in cache', fd, len, _pos);
            var _arg = {};
            vasync.pipeline({
                funcs: [
                    function _open(arg, _cb) {
                        fs.open(fname, 'r', function (err, _fd) {
                            if (err) {
                                _cb(err);
                            } else {
                                arg.fd = _fd;
                                _cb();
                            }
                        });
                    },
                    function _read(arg, _cb) {
                        fs.read(arg.fd, buf, off, len, _pos, function (err, nbytes) {
                            if (err) {
                                _cb(err);
                            } else {
                                arg.nbytes = nbytes;
                                _cb();
                            }
                        });
                    },
                    function _close(arg, _cb) {
                        fs.close(arg.fd, _cb);
                    }
                ],
                arg: _arg
            }, function onPipelineDone(err) {
                onReadDone(err, _arg.nbytes);
            });
            return;
        }

        log.trace('read(%d, %d, %d): data not in cache', fd, len, _pos);
        var _opts = {
            buffer: buf,
            offset: off,
            start: _pos,
            end: (_pos + len - 1)
        };
        self.manta.rangeGet(fmap.path, _opts, onReadDone);
    });
};



/**
 * Implementation of fs.readdir() [really readdir(3)].
 *
 * We always stat(2) first, so we ensure that info is cached,
 * and the LRU gets bumped
 */
MantaFs.prototype.readdir = function readdir(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    p = manta.path(p, true);
    cb = once(cb);

    var names = [];
    var self = this;

    this.cache.setEvict(p, false);

    function ensureIsDir(_arg, _cb) {
        _cb = once(_cb);
        self.stat(_arg.path, function onStat(err, stats) {
            if (err) {
                _cb(err);
            } else if (!stats.isDirectory()) {
                _cb(new ErrnoError('ENOTDIR', 'readdir'));
            } else {
                _arg.stats = stats;
                _cb();
            }
        });
    }

    function readFromCache(_arg, _cb) {
        if (_arg.done) {
            _cb();
            return;
        }

        _cb = once(_cb);
        self.cache.readdir(p, function onReadDirStream(err, dstream) {
            if (err) {
                _cb(err);
            } else if (!dstream) {
                _cb();
            } else {
                _arg.done = true;
                dstream.on('data', function onDirEnt(d) {
                    _arg.names.push(d);
                });
                dstream.once('end', function onDirStreamDone() {
                    _arg.names.sort();
                    _cb();
                });
            }
        });
    }

    function download(_arg, _cb) {
        _cb = once(_cb);
        if (_arg.done) {
            _cb();
        } else if (!self.cache.inFlight(p)) {
            var _opts = {
                path: _arg.stats._cacheFile
            };
            self.cache.inFlight(p, true);
            self.manta.cacheDirectory(_arg.path, _opts, function (err) {
                self.cache.inFlight(p, false);
                _cb(err);
            });
        } else { // wait for data
            // For readdir we just wait for the complete file
            self.cache.wait(_arg.path, _arg.stats, _arg.stats.size, cb);
        }
    }

    vasync.pipeline({
        funcs: [
            ensureIsDir,
            readFromCache,
            download,
            readFromCache
        ],
        arg: {
            names: names,
            path: p
        }
    }, function onReaddirPipelineDone(err) {
        self.cache.setEvict(p);
        if (err) {
            cb(err);
        } else {
            cb(null, names);
        }
    });
};


/**
 * Asynchronous rename(2). No arguments other than a possible exception are
 * given to the completion callback.
 *
 * This is a very complicated implementation (probably obviously). We
 * synchronously:
 *
 * - (manta) SnapLink the oldPath to newPath
 * - (manta) unlink oldPath
 * - Stage and Stat the new entry
 * - Stage in the parent
 * - unstage the old parent entry
 * - Delete the old fhandle mapping
 * - return to the user
 * - if the oldPath was cached and dirty, upload it
 * - unstage the old entry
 *
 * For those following along at home with the code below, it's in the reverse
 * order of the above (callback chaining makes it that way), so read it
 * bottom-up.
 *
 * This isn't a very quick way to use mv(1).
 */
MantaFs.prototype.rename = function rename(oldPath, newPath, cb) {
    assert.string(oldPath, 'oldPath');
    assert.string(newPath, 'newPath');
    assert.func(cb, 'callback');

    oldPath = manta.path(oldPath, true);
    newPath = manta.path(newPath, true);
    cb = once(cb);

    var self = this;

    function unstage() {
        var fhandlek = sprintf(FHANDLE_KEY_FMT, oldPath);
        self.db.del(fhandlek, function () {
            self._unstage(oldPath, cb);
        });
    }

    function upload() {
        var key = sprintf(DIRTY_KEY_FMT, oldPath);
        // XXX JJ - no db
        self.db.get(key, function (err, val) {
            unstage();
            if (err || !val || !val.local)
                return;

            var fstream = fs.createReadStream(val.local);
            self.manta.put(newPath, fstream, function (err2) {
                if (err2) {
                    self.log.error(err2, 'rename: failed to upload %s to %s',
                                   val.local, newPath);
                }

                fs.unlink(val.local, function () {
                    self.db.del(key, function () {
                        self.emit('upload', newPath);
                    });
                });
            });
        });
    }

    function stage() {
        self._ensureParent(newPath, function onParent(err, pstats) {
            if (err) {
                cb(err);
                return;
            }

            self._stat(newPath, false, function onStat(err2, stats) {
                if (err2) {
                    cb(err2);
                    return;
                }

                self._stageParent(newPath, stats, pstats, function (err3) {
                    if (err3) {
                        cb(err3);
                        return;
                    }

                    upload();
                });
            });
        });
    }

    function mantaMove() {
        self.manta.ln(oldPath, newPath, function onLink(err) {
            if (err) {
                cb(xlateMantaError(err, 'rename'));
                return;
            }

            // It's possible the old entry didn't exist in manta, so
            // we ignore errors
            self.manta.unlink(oldPath, stage);
        });
    }

    mantaMove();
};


/**
 * The unlink function is the common path for cleaning up.
 */
MantaFs.prototype.rmdir = function rmdir(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    p = manta.path(p, true);
    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('rmdir(%s): entered', p);

    this.stat(p, function (s_err, stats) {
        if (s_err) {
            log.trace(s_err, 'rmdir(%s): failed', p);
            cb(s_err);
        } else if (!stats.isDirectory()) {
            log.trace('rmdir(%s): ENOTDIR', p);
            cb(new ErrnoError('ENOTDIR', 'rmdir'));
        } else {
            self.unlink(p, function (err) {
                if (err) {
                    log.trace(err, 'rmdir(%s): failed', p);
                    cb(err);
                } else {
                    log.trace('rmdir(%s): ', p);
                    cb();
                }
            });
        }
    });
};


/**
 * Implementation of fs.stat() [really stat(2)].
 */
MantaFs.prototype.stat = function stat(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var self = this;

    this.cache.get(p, function onCacheGet(err, stats) {
        if (err) {
            err.syscall = 'stat';
            cb(err);
        } else if (!stats) {
            self._stat(p, function onStatDonw(err2, stats2) {
                if (err2) {
                    cb(err2);
                } else {
                    cb(null, mantaToStats(p, stats2));
                }
            });
        } else {
            cb(null, mantaToStats(p, stats));
        }
    });
};


/**
 * Shuts down the cache, and closes the internal leveldb.
 *
 * You can optionally pass a callback. The callback will receive any error
 * encountered during closing as the first argument.
 *
 * If no callback is provided, the fs instance will emit `close` or `error`.
 *
 */
MantaFs.prototype.shutdown = function shutdown(cb) {
    assert.optionalFunc(cb, 'callback');

    var self = this;

    Object.keys(this._timers).forEach(function (k) {
        clearTimeout(self._timers[k]);
        delete self._timers[k];
    });

    this.manta.close();
    if (this.cache) {
        this.cache.close(cb);
    } else if (cb) {
        setImmediate(cb);
    }
};


// TODO: honor the length argument
// TODO cancel outstanding download
MantaFs.prototype.truncate = function truncate(p, len, cb) {
    assert.string(p, 'path');
    assert.number(len, 'len');
    assert.func(cb, 'callback');

    p = manta.path(p, true);
    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('truncate(%s): entered', p);
    self._get_manta_info(p, function (ms_err, m_info) {
        if (ms_err) {
            log.trace(ms_err, 'truncate(%s): failed', p);
            cb(ms_err);
            return;
        }

        if (m_info.extension === 'directory') {
            ms_err = new ErrnoError('EISDIR', 'truncate');
            log.trace(ms_err, 'truncate(%s): isDirectory', p);
            cb(ms_err);
            return;
        }

        // set and save the new size
        m_info.size = 0;
        self._stat(p, m_info, function (s_err, info) {
            if (s_err) {
                log.trace(s_err, 'truncate(%s): failed (stat)', p);
                cb(s_err);
                return;
            }

            self.cache.truncate(p, m_info, function (t_err) {
                if (t_err) {
                    log.error(t_err, 'truncate(%s): failed (fs)', p);
                    cb(t_err);
                    return;
                }

                log.trace('truncate(%s): done', p);
                cb(null);
            });
        });
    });
};

// Get the manta info for a file from either the cache or via _stat which
// will have to go out and hit manta.
MantaFs.prototype._get_manta_info = function _get_manta_info(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    var log = this.log;
    var self = this;

    this.cache.get(p, function onCacheGet(err, m_info) {
        if (err) {
            log.trace(err, '_get_manta_info(%s): failed', p);
            cb(err);
            return;
        }

        if (!m_info) {
            self._stat(p, function onStatDonw(err2, stats) {
                if (err2) {
                    log.trace(err2, '_get_manta_info(%s): failed', p);
                    cb(err2);
                } else {
                    log.trace('_get_manta_info(%s): manta hit %j', p, stats);
                    cb(null, stats);
                }
            });
        } else {
            log.trace('_get_manta_info(%s): cache hit %j', p, m_info);
            cb(null, m_info);
        }
    });
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


/**
 * Cleaning up the cached data will lead to the manta call to unlink.
 */
MantaFs.prototype.unlink = function unlink(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    p = manta.path(p, true);
    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('unlink(%s): entered', p);
    self.cache.del(p, function (d_err) {
        if (d_err) {
            log.trace(d_err, 'unlink(%s): failed', p);
            cb(d_err);
        } else {
            self.cache.undirtyParent(p, function (err) {
                if (err) {
                    log.trace(err, 'unlink(%s): failed', p);
                    cb(err);
                } else {
                    log.trace('unlink(%s): done', p);
                    cb();
                }
            });
        }
    });
};


/**
 * Write buffer to the file specified by fd.
 *
 * offset and length determine the part of the buffer to be written.
 *
 * position refers to the offset from the beginning of the file where this data
 * should be written. If position is null, the data will be written at the
 * current position. See pwrite(2).
 *
 * The callback will be given three arguments (err, written, buffer) where
 * written specifies how many bytes were written from buffer.
 *
 * Note that it is unsafe to use fs.write multiple times on the same file
 * without waiting for the callback. For this scenario, fs.createWriteStream
 * is strongly recommended.
 *
 * On Linux, positional writes don't work when the file is opened in append
 * mode. The kernel ignores the position argument and always appends the data
 * to the end of the file.
 */
MantaFs.prototype.write = function write(fd, buf, off, len, pos, cb) {
    assert.number(fd, 'fd');
    assert.buffer(buf, 'buffer');
    assert.number(off, 'offset');
    assert.number(len, 'length');
    if (typeof (pos) === 'function') {
        cb = pos;
        pos = null;
    }
    if (pos !== null)
        assert.number(pos, 'position');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    if (!this._fds[fd]) {
        setImmediate(function invalidFD() {
            cb(new ErrnoError('EBADF', 'write'));
        });
        return;
    }

    var fmap = this._fds[fd];
    var fname = fmap.info._cacheFile;
    var _pos = (pos !== null ? pos : fmap.pos);

    log.trace('write(%d, %d, %d): entered', fd, len, pos);

    this.cache.dataInCache(fmap.path, _pos + len,
      function (c_err, ready, info) {
        if (c_err) {
            log.trace(c_err, 'write(%d, %d, %d): failed', fd, len, _pos);
            cb(c_err);
            return;
        }

        function onReady() {
            log.trace('write(%d, %d, %d): data is in cache', fd, len, _pos);

            var _arg = {};
            vasync.pipeline({
                funcs: [
                    function _open(arg, _cb) {
                        fs.open(fname, 'r+', function (err, _fd) {
                            if (err) {
                                _cb(err);
                            } else {
                                arg.fd = _fd;
                                _cb();
                            }
                        });
                    },
                    function _write(arg, _cb) {
                        fs.write(arg.fd, buf, off, len, _pos,
                          function (err, nbytes) {
                            if (err) {
                                _cb(err);
                            } else {
                                arg.nbytes = nbytes;
                                _cb();
                            }
                        });
                    },
                    function _close(arg, _cb) {
                        fs.close(arg.fd, _cb);
                    },
                    function _restat(arg, _cb) {
                        if ((_pos + len) >= info.size) {
                            info.size = _pos + len;
                            self._stat(fmap.path, info, _cb);
                        } else {
                            _cb();
                        }
                    }
                ],
                arg: _arg
            }, function onPipelineDone(err) {
                if (err) {
                    log.trace(err, 'write(%d, %d, %d): failed', fd, len, _pos);
                    cb(err);
                } else {
                    if (pos === null)
                        fmap.pos += _arg.nbytes;
                    log.trace(err, 'write(%d, %d, %d): done => %d',
                              fd, len, _pos, _arg.nbytes);

                    // mark file as dirty in the cache
                    self.cache.dirty(fmap.path, fmap.info._fhandle,
                      function (d_err) {
                        if (d_err) {
                            log.warn(d_err, 'dirty(%s): failed', fmap.path);
                            cb(d_err);
                        } else {
                            cb(null, _arg.nbytes, buf);
                        }
                    });
                }
            });
        }

        if (ready) {
            onReady();
        } else {
            log.trace('write(%d, %d, %d): data not in cache', fd, len, _pos);
            var _end = (_pos + len - 1);
            self.cache.wait(fmap.path, info, _end, function onWait(w_err) {
                if (w_err) {
                    log.trace(w_err, 'write(%d): failed (wait)', fd);
                    cb(w_err);
                } else {
                    onReady();
                }
            });
        }
    });
};



//-- Private Methods

/**
 * This method simply takes a given Manta path, and creates a local
 * cache file for it.  Control is returned to the user while the Manta
 * object is downloaded.  Errors do not need to be translated.
 */
MantaFs.prototype._cache = function _cache(_path, stats, cb) {
    assert.string(_path, 'path');
    assert.object(stats, 'stats');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.debug('_cache(%s): entered', _path);
    self.manta.get(_path, function onGet(m_err, mstream) {
        if (m_err) {
            log.debug(m_err, '_cache(%s): manta::get failed', _path);
            cb(xlateMantaError(m_err, '_cache'));
            return;
        }

        mstream.once('error', function onMantaError(err) {
            log.warn(err, '_cache: manta stream error from %s', _path);
            self._cleanupCacheFile(stats, fstream);
            cb(err);
        });

        stats._cacheFile = self._cacheFileName(stats);
        stats._pending = true;

        var fstream = fs.createWriteStream(stats._cacheFile);
        fstream.once('error', function onFileError(err) {
            log.warn(err, '_cache: error caching %s to %s',
                     _path,
                     stats._cacheFile);
            self._cleanupCacheFile(stats, fstream);
            mstream.unpipe(fstream);
            mstream.resume();
            cb(err);
        });
        fstream.once('finish', function onFileFinish() {
            log.debug('_cache(%s): file cached', _path);
            if (stats._pending)
                delete stats._pending;

            self.emit('cache', _path, stats);
        });

        mstream.pipe(fstream);

        fstream.once('open', function onCacheFileOpen() {
            log.debug('_cache(%s): manta stream caching', _path);
            cb();
        });
    });
};


MantaFs.prototype._cacheFileName = function _cacheFileName(stats) {
    assert.object(stats, 'stats');
    assert.string(stats._fhandle, 'stats._fhandle');

    return (path.join(this.path, 'fscache', stats._fhandle));
};


MantaFs.prototype._cleanupCacheFile = function _cleanCacheFile(stats, wstream) {
    assert.object(stats, 'stats');
    assert.optionalObject(wstream, 'writeStream');

    var log = this.log;
    var self = this;

    if (wstream) {
        wstream.removeAllListeners('finish');
        if (wstream.writable)
            wstream.end();
    }

    if (stats._cacheFile) {
        fs.unlink(stats._cacheFile, function (err) {
            if (err) {
                log.error({
                    err: err,
                    stats: stats
                }, '_cleanupCacheFile: unlink of %s failed', stats._path);
                setImmediate(self.emit.bind(self, 'error', err));
            }

            if (stats._cacheFile)
                delete stats._cacheFile;
            if (stats._pending)
                delete stats._pending;
        });
    }
};


MantaFs.prototype._ensureParent = function _ensureParent(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var dir = path.dirname(manta.path(p, true));
    var log = this.log;

    log.trace('_ensureParent(%s): entered', p);

    this._stat(dir, function (s_err, info) {
        if (s_err) {
            log.trace(s_err, '_ensureParent(%s): failed', p);
            cb(s_err);
            return;
        } else {
            log.trace('_ensureParent(%s): done', p);
            cb(null, info);
        }
    });

        // if (!stats._cacheFile) {
        //     self.readdir(dir, function (err, _, _stats) {
        //         cb(err, _stats);
        //     });
        // } else {
        // }

};


MantaFs.prototype._evict = function _evict(key, stats) {
    if (this._no_evict[key])
        return;

    var log = this.log;
    var self = this;

    function clearDB() {
        self.db.del(key, function (err) {
            if (err) {
                log.error(err, '_evict(%s): DB cleanup failed', key);
            } else {
                log.debug({
                    stats: stats
                }, '_evict(%s): cleanup done', key);
            }
        });
    }

    log.debug({
        stats: stats
    }, '_evict(%s): entered', key);
    if (!stats) {
        clearDB();
        return;
    }

    stats._fds.forEach(function (fd) {
        if (self._fds[fd])
            delete self._fds[fd];
    });

    if (!stats._cacheFile) {
        clearDB();
        return;
    }

    fs.unlink(stats._cacheFile, function (err) {
        if (err) {
            log.error({
                err: err,
                stats: stats
            }, '_evict(%s): failed to unlink %s', stats._cacheFile);

        } else {
            log.debug({
                stats: stats
            }, '_evict(%s): cached file (%s) unlinked', key, stats._cacheFile);
        }
        clearDB();
    });
};


/**
 * Ensures that we have a filename-to-filehandle mapping
 *
 * If the mapping already exists, we're done, otheriwse we create
 * and record a UUID format.
 *
 * Errors returned from this method need to be translated.
 *
 */
// MantaFs.prototype._fhandle = function _fhandle(_path, cb) {
//     assert.string(_path, 'path');
//     assert.func(cb, 'callback');

//     cb = once(cb);

//     var fhandle;
//     var k = sprintf(FHANDLE_KEY_FMT, _path);
//     var self = this;

//     this.db.get(k, function (err, val) {
//         if (!err && val) {
//             cb(null, val);
//             return;
//         }

//         fhandle = libuuid.create();
//         var k2 = sprintf(FNAME_KEY_FMT, fhandle);

//         self.db.batch()
//             .put(k, fhandle)
//             .put(k2, _path)
//             .write(function (err2) {
//                 if (err2) {
//                     cb(err2);
//                 } else {
//                     cb(null, fhandle);
//                 }
//             });
//     });
// };


/**
 * Adds this to the dirty queue if it doesn't already exist, otherwise
 * it just moves it back to the head.
 */
MantaFs.prototype._stage = function _stage(stats, cb) {
    assert.object(stats, 'stats');
    assert.func(cb, 'callback');

    cb = once(cb);

    var key = sprintf(DIRTY_KEY_FMT, stats._path);
    var self = this;
    // XXX JJ - this is old val stuff, cleanup
    // I need to be putting "stats" into the DB here
    var val = {
        local: stats._cacheFile,
        name: stats._path,
        size: stats.size,
        time: Date.now()
    };

    // XXX JJ still need this timeout handling?
//  clearTimeout(this._timers[key]);
//  this._timers[key] = setTimeout(this._upload.bind(this, key), this.wbtime);

    this.cache.put(stats._path, val, function onDbSave(err) {
        if (err) {
            clearTimeout(self._timers[key]);
            cb(err);
            return;
        }

        cb(null, val);
    });
};


/**
 * This is a private setter for setting stat() data.
 *
 * _stat gets invoked when we have either cache miss for a stat
 * call or a writer creates new data. This method will:
 *
 * - Perform a Manta HEAD on the specified path (if info=false)
 * - Stash ^^ in leveldb and in-memory LRUs
 *
 * Errors returned from this do not need to be translated.
 *
 */
MantaFs.prototype._stat = function _stat(p, info, cb) {
    assert.string(p, 'path');
    if (typeof info === 'function') {
        cb = info;
        info = false;
    }
    assert.ok(info !== undefined);
    assert.func(cb, 'callback');

    var log = this.log;
    var self = this;

    log.debug('_stat(%s, %j): entered', p, info);

    function save(_info) {
        self.cache.put(p, _info, function (err) {
            if (err) {
                log.debug(err, '_stat(%s): failed', p);
                cb(err);
            } else {
                log.debug(err, '_stat(%s): done', p);
                cb(null, _info);
            }
        });
    }

    if (!info) {
        this.manta.stat(p, function (err, _info) {
            if (err) {
                cb(err);
            } else {
                save(_info);
            }
        });
    } else {
        save(info);
    }
};


MantaFs.prototype._unstage = function _unstage(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var key = sprintf(DIRTY_KEY_FMT, _path);
    var log = this.log;
    var self = this;

    log.debug('_unstage(%s): entered', _path);

    if (this._timers[key]) {
        clearTimeout(this._timers[key]);
        delete this._timers[key];
    }

    this._unstageFromParent(_path, function (err) {
        if (err) {
            log.debug(err, '_unstage(%s): unstageFromParent failed', _path);
            cb(err);
            return;
        }

        log.debug('_unstage(%s): deleted from parent', _path);

        var dk = sprintf(DIRTY_KEY_FMT, _path);

        log.debug('_unstage(%s): deleting from dirty list', _path);
        self.db.del(dk, function () {
            log.debug('_unstage(%s): done', _path);
            cb();
        });
    });
};


MantaFs.prototype._unstageFromParent = function _unstageFromParent(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var k = sprintf(FILES_KEY_FMT, path.dirname(_path));
    var n = path.basename(_path);
    var log = this.log;
    var lstream;
    var rstream;
    var tmp;
    var wstream;

    // Drop this name from the cached parent directory listing
    if (!this.cache.has(k)) {
        cb();
        return;
    }

    this.cache.get(k, function onGet(err, stats) {
        if (err) {
            log.error(err, '_unstageFromParent(%s): error', k);
            cb(err);
            return;
        }

        if (!stats || !stats._cacheFile) {
            cb();
            return;
        }

        tmp = stats._cacheFile + '.tmp';
        lstream = new LineStream({
            encoding: 'utf8'
        });
        rstream = fs.createReadStream(stats._cacheFile, {
            encoding: 'utf8'
        });
        wstream = fs.createWriteStream(tmp, {
            encoding: 'utf8'
        });

        lstream.once('error', cb);
        rstream.once('error', cb);
        wstream.once('error', cb);

        // We want to write everything except for the current key
        lstream.on('data', function onDirectoryLine(line) {
            if (!line)
                return;

            try {
                var data = JSON.parse(line);
                if (data.name === n)
                    return;
            } catch (e) {
                log.error({
                    err: e,
                    file: stats._cacheFile,
                    line: line,
                    path: _path
                }, '_unstageFromParent: cache data corruption');
                return;
            }

            if (!wstream.write(line + '\n')) {
                wstream.once('drain', function onDrain() {
                    lstream.resume();
                });
                lstream.pause();
            }
        });

        lstream.once('end', function onDirectoryEnd() {
            wstream.once('finish', function onFlush() {
                fs.rename(tmp, stats._cacheFile, function onRename(err2) {
                    if (err2) {
                        log.error({
                            cacheFile: stats._cacheFile,
                            err: err2,
                            path: _path,
                            tmpFile: tmp
                        }, '_unstageFromParent: unable to rename tmp file');
                    }

                    cb(err2);
                });
            });
            wstream.end();
        });

        rstream.pipe(lstream);
    });
};


/**
 * Pushes a local cache file to Manta. Note this only occurs because the
 * file is dirty. We normally writeback once the dirty file has hit the 'age'
 * in the cache, but it can also happen because the cache size threshold was
 * exceeded. This code does not remove the local copy; it is an error to
 * remove the local copy when we're writing back due to hitting the 'age' in
 * the cache. If removal is needed, that must be done in the callback.
 */
MantaFs.prototype._upload = function _upload(cachedFile, mantaFile, cb) {
    assert.string(cachedFile, 'cachedFile');
    assert.string(mantaFile, 'mantaFile');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.debug('_upload(%s => %s): entered', cachedFile, mantaFile);

// XXX JJ
//    var k = sprintf(FILES_KEY_FMT, val.name);
//    self._no_evict[k] = true;
    var rstream = fs.createReadStream(cachedFile);
    rstream.once('error', function onFileError(err) {
        log.error(err, '_upload(%s): error reading from local file %s',
            mantaFile, cachedFile);
//        if (self._no_evict[k])
//            delete self._no_evict[k];
        cb(err);
    });

    rstream.once('open', function onOpen() {
        self.manta.put(mantaFile, rstream, function onPut(err) {
            if (err) {
                log.error(err, '_upload(%s): error pushing to manta(%s)',
                    cachedFile, mantaFile);
                cb(xlateMantaError(err));
            } else {
                log.debug('_upload(%s pushed to %s):', cachedFile, mantaFile);
                cb();
            }
//            if (self._no_evict[k])
//                delete self._no_evict[k];
        });
    });
};


// Waits for the cache file to have bytes up to "pos" (inclusive)
MantaFs.prototype._waitForData = function _waitForData(stats, pos, opts, cb) {
    assert.object(stats, 'stats');
    assert.number(pos, 'position');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    cb = once(cb);

    var k = sprintf(FILES_KEY_FMT, stats._path);
    var self = this;

    function _cb(err, needsRead) {
        if (self._no_evict[k])
            delete self._no_evict[k];

        cb(err, needsRead);
    }

    // bump the LRU, and mark this as safe
    this._no_evict[k] = true;
    this.cache.get(stats._path, function onGet(e, dummy) {
        if (e) {
            cb(e);
            return;
        }

        if (stats._pending) {
            // If we have the whole file, or if the current cached file
            // has enough data to satisfy the user's request we're G2G
            function ready(s) {
                return (!stats._pending || s.size === stats.size ||
                    s.size >= pos);
            }

            var fname = stats._cacheFile;
            fs.stat(fname, function onStat(err, stats2) {
                if (err) {
                    _cb(err);
                    return;
                }

                if (ready(stats2)) {
                    _cb();
                    return;
                }

                if (opts.nowait) {
                    _cb(null, true);
                    return;
                }
                // Wait for data - fs.watch() seems to be unreliable across
                // platforms (like Mac), so do this the ghetto way :(
                var interval = parseInt(process.env.FS_POLL_INTERVAL || 10, 10);
                setTimeout(function poll() {
                    fs.stat(fname, function onStatPoll(err2, stats3) {
                        if (err2) {
                            _cb(err2);
                        } else if (ready(stats3)) {
                            _cb();
                        } else {
                            setTimeout(poll, interval);
                        }
                    });
                }, interval);
            });
        } else if (!stats._cacheFile) {
            this._cache(stats._path, stats, function (err) {
                if (err) {
                    cb(err);
                } else {
                    self._waitForData(stats, pos, cb);
                }
            });
        } else {
            _cb();
        }
    });
};



///--- Exports

// module.exports = {
//     MantaFs: MantaFs,

//     createClient: function createClient(opts) {
//         assert.object(opts, 'options');

//         var _opts = {
//             dirty: opts.dirty || 1000,
//             wbtime: opts.wbtime || 60,
//             files: opts.files,
//             log: opts.log || bunyan.createLogger({
//                 stream: process.stderr,
//                 level: 'warn',
//                 name: 'MantaFs',
//                 serializers: bunyan.stdSerializers
//             }),
//             path: opts.path,
//             manta: opts.manta,
//             sizeMB: opts.sizeMB,
//             ttl: opts.ttl || 3600
//         };

//         return (new MantaFs(_opts));
//     }
// };
//     var _opts = {
//         nowait: true
//     };
//     var stats = map.stats;
//     var fname = stats._cacheFile;
//     pos = pos !== null ? pos : map.pos;

//     this._waitForData(stats, (pos + len), _opts, function (err, needsRead) {
//         if (err) {
//             cb(err);
//             return;
//         } else if (needsRead) {
//             _opts = {
//                 headers: {
//                     range: sprintf('bytes=%d-%d', pos, (pos + len - 1))
//                 }
//             };
//             self.manta.get(stats._path, _opts, function (m_err, rstream) {
//                 if (m_err) {
//                     cb(xlateMantaError(m_err));
//                     return;
//                 }

//                 var ndx = off;
//                 var total = 0;
//                 rstream.once('error', cb);
//                 rstream.on('data', function (chunk) {
//                     if (total > len)
//                         return;
//                     for (var i = 0; i < chunk.length; i++) {
//                         if (total <= len) {
//                             buf[ndx++] = chunk[i];
//                             map.pos++;
//                             total++;
//                         }
//                     }
//                 });
//                 rstream.once('end', function () {
//                     cb(null, total, buf);
//                 });
//                 rstream.resume();
//             });
//             return;
//         }

//         fs.open(fname, 'r', function onOpen(o_err, _fd) {
//             if (o_err) {
//                 cb(o_err);
//                 return;
//             }

//             if (len + pos > stats.size)
//                 len = stats.size - pos;
//             fs.read(_fd, buf, off, len, pos, function onRead(r_err, nbytes) {
//                 if (r_err) {
//                     cb(r_err);
//                     return;
//                 }

//                 map.pos += nbytes;
//                 fs.close(_fd, function onClose(c_err) {
//                     if (c_err) {
//                         cb(c_err);
//                     } else {
//                         cb(null, nbytes, buf);
//                     }
//                 });
//             });
//         });
//     });
// };
