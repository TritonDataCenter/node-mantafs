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

var nobody_uid;
var nobody_gid;

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
    stats.ino = crc32.calculate(_path);
    if (info.extension === 'directory') {
        stats.nlink = parseInt(info.headers['result-set-size'], 10);
        stats.isFile = _false;
        stats.isDirectory = _true;
        stats.mode = 0755;
        stats.size = 0;
        stats.mtime = new Date(info.last_modified);
    } else {
        stats.nlink = 1;
        stats.isFile = _true;
        stats.isDirectory = _false;
        stats.mode = 0644;
        stats.size = info.size;
        stats.mtime = new Date(info.headers['last-modified']);
    }
    stats.uid = nobody_uid;
    stats.gid = nobody_gid;
    stats.rdev = 0;
    stats.atime = new Date();
    stats.ctime = stats.mtime;

    stats.isBlockDevice = _false;
    stats.isCharacterDevice = _false;
    stats.isSymbolicLink = _false;
    stats.isFIFO = _false;
    stats.isSocket = _false;

    stats._cacheFile = stats._cacheFile || info._cacheFile;
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
    assert.number(opts.uid, 'uid');
    assert.number(opts.gid, 'gid');

    events.EventEmitter.call(this, opts);

    this.cache = opts.cache;

    this.log = opts.log.child({component: 'MantaFsApi'}, true);
    this.manta = opts.manta;
    nobody_uid = opts.uid;
    nobody_gid = opts.gid;

    this._fd = 3;
    this._fds = {};

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
 */
//MantaFs.prototype.createReadStream = function createReadStream(_path, opts) {
//    assert.string(_path);
//    assert.optionalObject(opts);
//    opts = opts || {};
//    assert.optionalString(opts.flags, 'options.flags');
//    if (opts.encoding !== null)
//        assert.optionalString(opts.encoding, 'options.encoding');
//    assert.optionalNumber(opts.fd, 'options.fd');
//    assert.optionalNumber(opts.mode, 'options.mode');
//    assert.optionalBool(opts.autoClose, 'options.autoClose');
//
//    var _opts = {
//        autoClose: opts.autoClose,
//        encoding: opts.encoding,
//        fs: this,
//        start: opts.start,
//        end: opts.end
//    };
//    var rstream;
//
//    if (opts.fd) {
//        _opts.fd = opts.fd;
//        rstream = new ReadStream(_opts);
//    } else {
//        rstream = new ReadStream(_opts);
//        this.open(_path,
//                  (opts.flags || 'r'),
//                  (opts.mode || 0666),
//                  function onOpen(err, fd) {
//                      if (err) {
//                          rstream.emit('error', err);
//                      } else {
//                          rstream._open(fd);
//                      }
//                  });
//    }
//
//    return (rstream);
//};


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
 */
//MantaFs.prototype.createWriteStream =
//  function createWriteStream(_path, opts) {
//    assert.string(_path);
//    assert.optionalObject(opts);
//    opts = opts || {};
//    assert.optionalString(opts.flags, 'options.flags');
//    if (opts.encoding !== null)
//        assert.optionalString(opts.encoding, 'options.encoding');
//    assert.optionalNumber(opts.mode, 'options.mode');
//
//    var _opts = {
//        encoding: opts.encoding || null,
//        flags: opts.flags || 'w',
//        fs: this,
//        mode: opts.mode || 0666,
//        start: opts.start
//    };
//    var wstream = new WriteStream(_opts);
//
//    this.open(_path, _opts.flags, _opts.mode, function onOpen(err, fd) {
//        if (err) {
//            wstream.emit('error', err);
//        } else {
//            wstream._open(fd);
//        }
//    });
//
//    return (wstream);
//};

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
 * Pushes a file into manta synchronously (if its dirty); this is expensive,
 * so be sure that you want to do this.
 */
MantaFs.prototype.fsync = function fsync(fd, cb) {
    assert.number(fd, 'fd');
    assert.func(cb, 'callback');

    if (!(this._fds[fd])) {
        setImmediate(cb.bind(this, new ErrnoError('EBADF', 'fsync')));
    } else {
        this.cache.writeback_now(this._fds[fd].path, cb);
    }
};


/**
 * Manta doesn't support setting the atime or mtime on an object, but we
 * use this as an indication that the caller wants to refresh the cache.
 */
MantaFs.prototype.utimes = function utimes(p, atime, mtime, cb) {
    assert.string(p, 'p');
    assert.number(atime, 'atime');
    assert.number(mtime, 'mtime');
    assert.func(cb, 'callback');

    var log = this.log;

    log.trace('utimes(%s): entered', p);
    this.cache.refresh(p, function (err) {
        if (err) {
            log.error(err, 'utimes cache refresh (%s): failed', p);
            err.syscall = 'stat';
            cb(err);
            return;
        }

        log.trace('utimes(%s): done', p);
        cb(null);
    });
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

    this.cache.lookup(p, cb);
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

        self.manta.mkdir(manta.path(p, true), function onMkdir(m_err, info) {
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

                self.cache.add_to_pdir(p, function (err) {
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

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('open(%s, %s, %d): entered', p, flags, mode);

    function _checkForDownload(_arg, _cb) {
        if (self.cache.inFlight(p) || _arg.create) {
            _cb();
            return;
        }

        self.stat(p, function (s_err, info) {
            if (s_err) {
                if (s_err.code === 'ENOENT' && _arg.create) {
                    _arg.run_create = true;
                    _cb();
                } else {
                    _cb(s_err);
                }
            } else {
                _arg.info = info._manta;
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

        var opts = {};
        if (_arg.exclusive) {
            // exclusive applies only to the local file in the cache
            opts.flag = 'wx';
        } else {
            opts.flag = 'w';
        }

        self._stat(p, info, function (s_err, stats) {
            if (s_err) {
                _cb(s_err);
                return;
            }

            fs.writeFile(stats._cacheFile, new Buffer(0), opts, function (err) {
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
                        self.cache.add_to_pdir(p, _cb);
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
        self.cache.inFlight(p, true, _arg.info.size);
        self.manta.cacheObject(manta.path(p, true), _opts, function () {
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

    // exclusive applies only to the local file in the cache
    case 'wx':
        _ensureCacheFile({
            create: true,
            exclusive: true
        });
        break;

    default:
        cb(new ErrnoError('EINVAL', 'open'));
        break;
    }
};


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
                        fs.open(info._cacheFile, 'r', function (err, _fd) {
                            if (err) {
                                _cb(err);
                            } else {
                                arg.fd = _fd;
                                _cb();
                            }
                        });
                    },
                    function _read(arg, _cb) {
                        fs.read(arg.fd, buf, off, len, _pos,
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
        self.manta.rangeGet(manta.path(fmap.path, true), _opts, onReadDone);
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
            // always just estimate the dir size
            var size = 5000;
            self.cache.inFlight(p, true, size);
            self.manta.cacheDirectory(manta.path(_arg.path, true), _opts,
              function (err) {
                self.cache.load_cache_from_dir(p, function (err2) {
                    self.cache.inFlight(p, false);
                    _cb(err);
                });
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
 * - if the oldPath was cached and dirty, upload it
 * - (manta) SnapLink the oldPath to newPath
 * - Stage in the parent
 * - fix any open file descriptors
 * - the cache mv code does the rest of the work to handle rename
 */
MantaFs.prototype.rename = function rename(oldPath, newPath, cb) {
    assert.string(oldPath, 'oldPath');
    assert.string(newPath, 'newPath');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    function mantaMove() {
        self.unlink(newPath, function (err) {
            if (err) {
                log.trace(err, 'rmdir(%s): failed', newPath);
                cb(err);
                return;
            }

            self.manta.ln(manta.path(oldPath, true), manta.path(newPath, true),
              function onLink(err2) {
                if (err2) {
                    cb(xlateMantaError(err2, 'rename'));
                    return;
                }

                self._ensureParent(newPath, function onParent(err3) {
                    if (err3) {
                        cb(err3);
                        return;
                    }

                    for (var fd in self._fds) {
                        if (self._fds[fd].path === oldPath) {
                            self._fds[fd].path = newPath;
                            // can't break since there could be more than one
                        }
                    }

                    self.cache.mv(oldPath, newPath, cb);
                });
            });
        });
    }

    log.trace('rename(%s, %s): entered', oldPath, newPath);
    self.cache.writeback_now(oldPath, mantaMove);
};


/**
 * The unlink function is the common path for cleaning up.
 */
MantaFs.prototype.rmdir = function rmdir(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

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
            self.readdir(p, function (rd_err, files) {
                if (rd_err) {
                    log.trace(rd_err, 'rmdir read (%s): failed', p);
                    cb(rd_err);
                    return;
                }

                if (files.length !== 0) {
                    log.trace('rmdir(%s): ENOTEMPTY', p);
                    cb(new ErrnoError('ENOTEMPTY', 'rmdir'));
                    return;
                }

                self.unlink(p, function (err) {
                    if (err) {
                        log.trace(err, 'rmdir(%s): failed', p);
                        cb(err);
                    } else {
                        log.trace('rmdir(%s): ', p);
                        cb();
                    }
                });
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
            self._stat(p, function onStatDone(err2, stats2) {
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

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('truncate(%s): entered', p);
    self.stat(p, function (ms_err, m_info) {
        if (ms_err) {
            log.trace(ms_err, 'truncate(%s): failed', p);
            cb(ms_err);
            return;
        }

        if (m_info._manta.extension === 'directory') {
            ms_err = new ErrnoError('EISDIR', 'truncate');
            log.trace(ms_err, 'truncate(%s): isDirectory', p);
            cb(ms_err);
            return;
        }

        // set and save the new size
        m_info._manta.size = 0;
        self._stat(p, m_info._manta, function (s_err, info) {
            if (s_err) {
                log.trace(s_err, 'truncate(%s): failed (stat)', p);
                cb(s_err);
                return;
            }

            self.cache.truncate(p, m_info._manta, function (t_err) {
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

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('unlink(%s): entered', p);
    self.cache.del(p, function (d_err) {
        if (d_err) {
            log.trace(d_err, 'unlink(%s): failed', p);
            cb(d_err);
        } else {
            log.trace('unlink(%s): done', p);
            cb();
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
                        fs.open(info._cacheFile, 'r+', function (err, _fd) {
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
                        var now = new Date();
                        info.headers['last-modified'] = now.toUTCString();
                        var osize = info.size;
                        if ((_pos + len) >= info.size) {
                            info.size = _pos + len;
                            self.cache.extend(fmap.path, info.size - osize);
                        }
                        self._stat(fmap.path, info, _cb);
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
                    self.cache.dirty(fmap.path, info._fhandle,
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
MantaFs.prototype._download = function _download(_path, stats, cb) {
    assert.string(_path, 'path');
    assert.object(stats, 'stats');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.debug('_download(%s): entered', _path);
    self.manta.get(manta.path(_path, true), function onGet(m_err, mstream) {
        if (m_err) {
            log.debug(m_err, '_download(%s): manta::get failed', _path);
            cb(xlateMantaError(m_err, '_download'));
            return;
        }

        mstream.once('error', function onMantaError(err) {
            log.warn(err, '_download: manta stream error from %s', _path);
            self._cleanupCacheFile(stats, fstream);
            cb(err);
        });

        stats._cacheFile = self._cacheFileName(stats);
        stats._pending = true;

        var fstream = fs.createWriteStream(stats._cacheFile);
        fstream.once('error', function onFileError(err) {
            log.warn(err, '_download: error caching %s to %s',
                     _path,
                     stats._cacheFile);
            self._cleanupCacheFile(stats, fstream);
            mstream.unpipe(fstream);
            mstream.resume();
            cb(err);
        });
        fstream.once('finish', function onFileFinish() {
            log.debug('_download(%s): file cached', _path);
            if (stats._pending)
                delete stats._pending;

            self.emit('cache', _path, stats);
        });

        mstream.pipe(fstream);

        fstream.once('open', function onCacheFileOpen() {
            log.debug('_download(%s): manta stream caching', _path);
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


// Make sure parent dir is cached
MantaFs.prototype._ensureParent = function _ensureParent(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var dir = path.dirname(p);
    var log = this.log;

    log.trace('_ensureParent(%s): entered', p);

    this.stat(dir, function (s_err, info) {
        if (s_err) {
            log.trace(s_err, '_ensureParent(%s): failed', p);
            cb(s_err);
            return;
        } else {
            log.trace('_ensureParent(%s): done', p);
            cb(null);
        }
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
    if (typeof (info) === 'function') {
        cb = info;
        info = false;
    }
    assert.ok(info !== undefined);
    assert.func(cb, 'callback');

    var log = this.log;
    var self = this;

    log.debug('_stat(%s, %j): entered', p, info);

    function save(_info) {
        self.cache.put(p, _info, function (err, c_info) {
            if (err) {
                log.debug(err, '_stat(%s): failed', p);
                cb(err);
            } else {
                log.debug(err, '_stat(%s): done', p);
                cb(null, c_info);
            }
        });
    }

    if (!info) {
        self.manta.stat(manta.path(p, true), function (err, _info) {
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
