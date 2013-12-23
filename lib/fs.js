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
var ReadStream = require('./readable').ReadStream;
var WriteStream = require('./writable').WriteStream;
var utils = require('./utils');



///--- Globals

var sprintf = util.format;
var ErrnoError = errors.ErrnoError;
var xlateDBError = errors.xlateDBError;
var xlateMantaError = errors.xlateMantaError;

var FHANDLE_KEY_FMT = '::fhandles:%s';
var FNAME_KEY_FMT = '::fnames:%s';
var FILES_KEY_FMT = '::files:%s';
var DIRTY_KEY_FMT = '::dirty:%s';



///--- Cache Override

// We know that all values going into set/get are the results of
// Manta.info() calls, so we do our own age management on top of
// the LRU TTL (based on cache-control)
function createCache(opts) {
    assert.object(opts, 'options');

    var lru = LRU(opts);

    var _get = lru.get.bind(lru);

    function get(k) {
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

    lru.get = get;

    return (lru);
}



///--- Helpers

// Increments our fd - we start at 3
function incr(i) {
    if (++i >= Math.pow(2, 32) - 1)
        i = 3;
    return (i);
}


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

        if (stats.statvfs.availableMB < MB(thisp.max_size)) {
            log.warn('%s has %dMB available. Using as max size',
                     thisp.path, stats.statvfs.availableMB);

            thisp.max_size = bytes(stats.statvfs.availableMB);
        }

        thisp.db = levelup(thisp.path + '/manta.db', {
            valueEncoding: 'json'
        });

        thisp.db.on('error', thisp.emit.bind(thisp, 'error'));
        thisp.db.once('ready', function onDatabase() {
            var keys = thisp.db.createReadStream();
            keys.on('data', function (data) {
                /* JSSTYLED */
                if (!/^::files:/.test(data.key)) // fix syntax hilights :)
                    return;

                var k = data.key.split(':').pop();
                var d = data.value;
                thisp.size += data.value.size;
                thisp.cache.set(data.key, mantaToStats(k, d));
            });
            keys.once('error', thisp.emit.bind(thisp, 'error'));
            keys.once('end', thisp.emit.bind(thisp, 'ready'));
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
    // XXX (broken, same for all files) stats.ino = crc32.calculate(info.etag);
    stats.ino = crc32.calculate(_path);
    if (info.extension === 'directory') {
        stats.nlink = parseInt(info.headers['result-set-size'], 10);
        stats.isFile = _false;
        stats.isDirectory = _true;
        stats.mode = 0755;
        stats.size = 0; // XXX makeup something else?
        stats.mtime = new Date(); // XXX can we do better?
    } else {
        stats.nlink = 1;
        stats.isFile = _true;
        stats.isDirectory = _false;
        stats.mode = 0644;
        stats.size = info.size;
        stats.mtime = new Date(info.headers['last-modified']);
    }
    stats.uid = -2;    // TODO: this is Mac-only
    stats.gid = -2;    // TODO: this is Mac-only
    stats.rdev = 0;
    stats.atime = new Date();
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
 * - dirty<Number>: Maximum number of dirty files to allow
 * - dirtyAge<Number>: Maximum time a file can stay dirty
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
    assert.number(opts.dirty, 'options.dirty');
    assert.number(opts.dirtyAge, 'options.dirtyAge');
    assert.number(opts.files, 'options.files');
    assert.object(opts.log, 'options.log');
    assert.object(opts.manta, 'options.manta');
    assert.string(opts.path, 'options.path');
    assert.number(opts.sizeMB, 'options.sizeMB');
    assert.number(opts.ttl, 'options.ttl');

    events.EventEmitter.call(this, opts);

    this.cache = createCache({
        dispose: this._evict.bind(this),
        max: opts.files,
        maxAge: opts.ttl * 1000
    });

    this.dirtyAge = opts.dirtyAge * 1000;
    this.log = opts.log.child({component: 'MantaFs'}, true);
    this.manta = opts.manta;
    this.max_files = opts.files;
    this.max_size = bytes(opts.sizeMB);
    this.path = path.normalize(opts.path);
    this.size = 0;
    this.ttl = opts.ttl * 1000;

    this._fd = 3;
    this._fds = {};

    // LRU eviction is asynchronous, and sometimes triggered when
    // we don't want it (like in readdir/stat). So we have to do
    // some book-keeping so that evictions/uploads skip entries we know
    // are "hot"
    this._no_evict = {};
    this._no_upload = {};
    this._timers = {};

    init(this);
}
util.inherits(MantaFs, events.EventEmitter);


/**
 * Closes an `fd` that was obtained (and used) via `open`.
 */
MantaFs.prototype.close = function close(fd, cb) {
    assert.number(fd, 'fd');
    assert.func(cb, 'callback');

    var ndx;
    var stats;
    if (!(this._fds[fd])) {
        setImmediate(cb.bind(this, new ErrnoError('EBADF', 'close')));
        return;
    }

    stats = this._fds[fd].stats;

    ndx = stats._fds.indexOf(fd);
    assert.ok(ndx >= 0);
    stats._fds.splice(ndx, 1);
    delete this._fds[fd];

    setImmediate(cb);
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
 */
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
 */
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


/**
 * Bonus operation that translates an opaque file handle back to
 * the original path (useful for NFS).
 */
MantaFs.prototype.fhandle_to_path = function fhandle_to_path(fhandle, cb) {
    assert.string(fhandle, 'fhandle');
    assert.func(cb, 'callback');

    cb = once(cb);

    var k = sprintf(FNAME_KEY_FMT, fhandle);

    this.db.get(k, function (err, val) {
        cb(err, val);
        return;
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
MantaFs.prototype.lookup = function lookup(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    _path = this.manta.path(_path, true);
    cb = once(cb);

    var k = sprintf(FHANDLE_KEY_FMT, _path);
    this.db.get(k, function onDbGet(err, val) {
        if (err) {
            cb(xlateDBError(err, 'lookup'));
        } else {
            cb(null, val);
        }
    });
};


/**
 * In the `mkdir` operation, we just go ahead and always call the
 * Manta API, as this is cheap, and saves us conflicts down the road.
 *
 */
MantaFs.prototype.mkdir = function mkdir(_path, mode, cb) {
    assert.string(_path, 'path');
    if (typeof (mode) === 'function') {
        cb = mode;
        mode = 0777;
    }
    assert.func(cb, 'callback');

    _path = this.manta.path(_path, true);
    cb = once(cb);

    var self = this;

    this._ensureParent(_path, function (p_err, stats) {
        if (p_err) {
            cb(p_err);
            return;
        }

        self.manta.mkdir(_path, function onMkdir(err, res, info) {
            if (err) {
                cb(xlateMantaError(err, 'mkdir'));
                return;
            }

            self._stat(_path, info, function (err2) {
                if (err2) {
                    cb(err2);
                    return;
                }

                self._stageParent(_path, info, stats, cb);
            });
        });
    });
};


/**
 * Implementation of fs.open()
 *
 * This will stat a file, and if it does not exist in the cache,
 * automatically start streaming it down.
 *
 * Writes not currently supported.
 */
MantaFs.prototype.open = function open(_path, flags, mode, cb) {
    assert.string(_path, 'path');
    assert.string(flags, 'flags');
    if (typeof (mode) === 'function') {
        cb = mode;
        mode = 0666;
    }
    assert.number(mode, 'mode');
    assert.func(cb, 'callback');

    _path = this.manta.path(_path, true);
    cb = once(cb);

    var log = this.log;
    var self = this;

    log.debug('open(%s, %s, %d): entered', _path, flags, mode);

    function assign_fd(stats, err) {
        if (err) {
            cb(err);
        } else {
            self._fd = incr(self._fd);
            self._fds[self._fd] = {
                flags: flags,
                pos: /a/.test(flags) ? stats.size : 0,
                stats: stats
            };
            stats._fds.push(self._fd);
            cb(null, self._fd);
        }
    }

    // We ensure the parent exists, create a zero byte file to
    // avoid conflicts, and then _stat the new guy
    function creat() {
        self._ensureParent(_path, function (parent_err, pstats) {
            if (parent_err) {
                cb(parent_err);
                return;
            }

            var z = new stream.PassThrough();
            self.manta.put(_path, z, function (p_err) {
                if (p_err) {
                    cb(xlateMantaError(p_err, 'open'));
                    return;
                }

                // Stubbing this out saves a trip to manta
                var info = {
                    extension: 'bin',
                    type: 'application/octet-stream',
                    etag: libuuid.create(),
                    parent: path.dirname(_path),
                    size: 0,
                    headers: {
                        'last-modified': new Date().toJSON()
                    }
                };
                self._stat(_path, info, function (s_err, stats) {
                    if (s_err) {
                        cb(s_err);
                        return;
                    }

                    var data = new Buffer(0);
                    var fname = self._cacheFileName(stats);
                    fs.writeFile(fname, data, function (w_err) {
                        if (w_err) {
                            cb(w_err);
                            return;
                        }

                        stats._cacheFile = fname;
                        var _assign_fd = assign_fd.bind(self, stats);
                        self._stageParent(_path, info, pstats, _assign_fd);
                    });
                });
            });
            z.end();
        });
    }

    this.stat(_path, function onStat(s_err, stats) {
        if (s_err) {
            if (s_err.code === 'ENOENT' && /w/.test(flags)) {
                log.debug('open(%s): need to create file', _path);
                creat();
            } else {
                log.debug(s_err, 'open(%s): stat error', _path);
                cb(s_err);
            }
            return;
        }

        var _assign_fd = assign_fd.bind(self, stats);
        // Anything with 's' is a LIE!
        switch (flags) {
        case 'a':
        case 'a+':
        case 'r':
        case 'r+':
        case 'rs':
        case 'rs+':
            if (!stats._cacheFile) {
                self._cache(_path, stats, _assign_fd);
            } else {
                _assign_fd();
            }
            break;

        case 'w':
        case 'w+':
            self.truncate(_path, 0, _assign_fd);
            break;

        default:
            cb(new ErrnoError('EINVAL', 'open'));
            break;
        }
    }, true);
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

    var map;

    if (!(map = this._fds[fd])) {
        setImmediate(function invalidFD() {
            cb(new ErrnoError('EBADF', 'read'));
        });
        return;
    }

    var stats = map.stats;
    var fname = stats._cacheFile;
    pos = pos !== null ? pos : map.pos;

    this._waitForData(stats, (pos + len), function _read(err) {
        fs.open(fname, 'r', function onOpen(o_err, _fd) {
            if (o_err) {
                cb(o_err);
                return;
            }

            if (len + pos > stats.size)
                len = stats.size - pos;
            fs.read(_fd, buf, off, len, pos, function onRead(r_err, nbytes) {
                if (r_err) {
                    cb(r_err);
                    return;
                }

                map.pos += nbytes;
                fs.close(_fd, function onClose(c_err) {
                    if (c_err) {
                        cb(c_err);
                    } else {
                        cb(null, nbytes, buf);
                    }
                });
            });
        });
    });
};


/**
 * Implementation of fs.readdir() [really readdir(3)].
 *
 */
MantaFs.prototype.readdir = function readdir(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    _path = this.manta.path(_path, true);
    cb = once(cb);

    var names = [];
    var self = this;

    // We always stat(2) first, so we ensure that info is cached,
    // and the LRU gets bumped
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

            lstream.once('end', function onEnd() {
                names.sort();
                cb(null, names, stats);
            });
            inf.pipe(lstream);
            return;
        }

        // cache miss
        var cacheFile = self._cacheFileName(stats);
        var out = fs.createWriteStream(cacheFile);
        out.once('error', cb);
        out.once('open', function onFileOpen() {
            self.manta.ls(_path, function onLsStart(ls_err, res) {
                if (ls_err) {
                    cb(xlateMantaError(ls_err, 'readdir'));
                    return;
                }

                res.once('error', function onLsError(err) {
                    cb(xlateMantaError(ls_err, 'readdir'));
                });

                res.on('entry', function onLsEntry(e) {
                    names.push(e.name);
                    if (!out.write(JSON.stringify(e) + '\n')) {
                        out.once('drain', res.resume.bind(res));
                        res.pause();
                    }
                });

                res.once('end', function onLsDone() {
                    out.once('finish', function onFileWritten() {
                        var key = sprintf(FILES_KEY_FMT, _path);
                        self.db.get(key, function onDbGet(db_err, info) {
                            if (db_err) {
                                cb(xlateDBError(db_err, 'readdir'));
                                return;
                            }

                            info._cacheFile = cacheFile;
                            stats._cacheFile = cacheFile;
                            self.db.put(key, info, function onDbWrite(err) {
                                if (err) {
                                    cb(xlateDBError(err, 'readdir'));
                                } else {
                                    self._no_evict[key] = true;
                                    self.cache.set(key, stats);
                                    delete self._no_evict[key];
                                    names.sort();
                                    cb(null, names, stats);
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

    oldPath = this.manta.path(oldPath, true);
    newPath = this.manta.path(newPath, true);
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
 * In the `rmdir` operation, we just always call Manta in-line.
 */
MantaFs.prototype.rmdir = function rmdir(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    _path = this.manta.path(_path, true);
    cb = once(cb);
    var self = this;

    this.stat(_path, function (err, stats) {
        if (err) {
            cb(err);
        } else if (!stats.isDirectory()) {
            cb(new ErrnoError('ENOTDIR', 'rmdir'));
        } else {
            self.unlink(_path, cb);
        }
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
        _path = this.manta.path(_path, true);

    var key = sprintf(FILES_KEY_FMT, _path);
    var stats;

    if ((stats = this.cache.get(key))) {
        if (internal) {
            cb(null, stats);
        } else {
            setImmediate(cb.bind(this, null, stats));
        }
    } else {
        this._stat(_path, false, cb);
    }
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

    var log = this.log;
    var self = this;

    var _cb = once(function (err) {
        log.debug(err, 'shutdown: %s', err ? 'failed' : 'done');
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

    log.debug('shutdown: entered');

    Object.keys(this._timers).forEach(function (k) {
        clearTimeout(self._timers[k]);
        delete self._timers[k];
    });

    if (this.db) {
        this.db.close(_cb);
    } else {
        _cb();
    }
};


MantaFs.prototype.truncate = function truncate(_path, len, cb) {
    assert.string(_path, 'path');
    assert.number(len, 'len');
    assert.func(cb, 'callback');

    _path = this.manta.path(_path, true);
    cb = once(cb);

    var self = this;

    this.stat(_path, function (s_err, stats) {
        if (s_err) {
            cb(s_err);
            return;
        }

        self._waitForData(stats, stats.size, function (w_err) {
            if (w_err) {
                cb(w_err);
                return;
            }

            fs.open(stats._cacheFile, 'r+', function (o_err, _fd) {
                if (o_err) {
                    cb(o_err);
                    return;
                }

                fs.truncate(_fd, len, function (t_err) {
                    fs.close(_fd, function (err) {
                        if (t_err || err) {
                            cb(t_err || err);
                            return;
                        }

                        stats.size = 0;
                        self._stage(stats, cb);
                    });
                });
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
 * In the `unlink` and `rmdir` operation, we just always call Manta in-line.
 */
MantaFs.prototype.unlink = function unlink(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    _path = this.manta.path(_path, true);
    cb = once(cb);

    var key = sprintf(FILES_KEY_FMT, _path);
    var log = this.log;
    var self = this;

    function _unlink() {
        self.manta.unlink(_path, function (err) {
            if (err) {
                cb(xlateMantaError(err, 'unlink'));
                return;
            }

            var fhandlek = sprintf(FHANDLE_KEY_FMT, _path);
            self.db.del(fhandlek, function (db_err) {
                // We ignore and kee
                if (db_err) {
                    log.error(db_err,
                              'unlink(%s): unable to delete fhandle mapping',
                              _path);
                }

                // LRU eviction will trigger cleanup of the cached data
                self.cache.del(key);
                self._unstage(_path, cb);
            });
        });
    }

    // If the file is cached we're fine. But it might not be.
    if (this.cache.has(key)) {
        _unlink();
        return;
    }

    this.stat(_path, function (err, stats) {
        if (err) {
            cb(err);
        } else {
            _unlink();
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

    var map;
    var self = this;

    if (!(map = this._fds[fd])) {
        setImmediate(function invalidFD() {
            cb(new ErrnoError('EBADF', 'write'));
        });
        return;
    }

    var _pos = pos !== null ? pos : map.pos;
    var stats = map.stats;

    this._waitForData(stats, (pos + len), function _write(err) {
        if (err) {
            cb(err);
            return;
        }

        fs.open(stats._cacheFile, map.flags, function onOpen(o_err, _fd) {
            if (o_err) {
                cb(o_err);
                return;
            }

            fs.write(_fd, buf, off, len, _pos, function onWrite(w_err, n, b) {
                if (w_err) {
                    cb(w_err);
                    return;
                }

                if (pos === null)
                    map.pos += n;

                fs.close(_fd, function onClose(c_err) {
                    if (c_err) {
                        cb(c_err);
                    } else {
                        fs.stat(stats._cacheFile, function (stat_err, _stats) {
                            if (stat_err) {
                                cb(stat_err);
                                return;
                            }

                            stats.size = _stats.size;
                            self._stage(stats, function (stage_err) {
                                if (stage_err) {
                                    cb(stage_err);
                                } else {
                                    cb(null, n, buf);
                                }
                            });
                        });
                    }
                });
            });
        });
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

    return (path.join(this.path, stats._fhandle));
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


MantaFs.prototype._ensureParent = function _ensureParent(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    var dir = path.dirname(_path);
    var self = this;

    this.stat(dir, function (s_err, stats) {
        if (s_err) {
            cb(s_err);
            return;
        }

        if (!stats._cacheFile) {
            self.readdir(dir, function (err, _, _stats) {
                cb(err, _stats);
            });
        } else {
            cb(null, stats);
        }
    });
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
                    cb(err2);
                } else {
                    cb(null, fhandle);
                }
            });
    });
};


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
    var val = {
        local: stats._cacheFile,
        name: stats._path,
        size: stats.size,
        time: Date.now()
    };

    clearTimeout(this._timers[key]);
    this._timers[key] = setTimeout(this._upload.bind(this, key), this.dirtyAge);

    this.db.put(key, val, function onDbSave(err) {
        if (err) {
            clearTimeout(self._timers[key]);
            cb(err);
            return;
        }

        cb(null, val);
    });
};


/**
 * Internal-only method that writes a dirent to the parent directory. It is
 * assumed that _ensureParent was already called.  You provide this the
 * new path, the manta "info" object, the parent's fs.Stats object and a
 * callback.
 */
MantaFs.prototype._stageParent = function _stageParent(p, info, pstats, cb) {
    assert.string(p, 'path');
    assert.object(info, 'info');
    assert.object(pstats, 'parent_stats');
    assert.func(cb, 'callback');

    var data;
    var fname = pstats._cacheFile;

    try {
        data = '\n' + JSON.stringify({
            name: path.basename(p),
            type: info.extension === 'directory' ? 'directory' : 'object',
            mtime: new Date().toJSON()
        }) + '\n';
    } catch (e) {
        cb(e);
        return;
    }

    fs.appendFile(fname, data, {encoding: 'utf8'}, cb);
};


/**
 * This is a private setter for setting stat() data.
 *
 * _stat gets invoked when we have either cache miss for a stat
 * call or a writer creates new data. This method will:
 *
 * - Ensure the file path has an `fhandle` mapping (and vice versa)
 * - Perform a Manta HEAD on the specified path (if info=false)
 * - Stash ^^ in leveldb and in-memory LRUs
 *
 * Errors returned from this do not need to be translated.
 *
 */
MantaFs.prototype._stat = function _stat(_path, info, cb) {
    assert.string(_path, 'path');
    assert.ok(info !== undefined);
    assert.func(cb, 'callback');

    var self = this;

    this._fhandle(_path, function onFhandle(f_err, fhandle) {
        if (f_err) {
            cb(xlateDBError(f_err, 'stat'));
            return;
        }

        function save(val) {
            var key = sprintf(FILES_KEY_FMT, _path);
            var stats = mantaToStats(_path, val);

            val._fhandle = fhandle;
            stats._fhandle = fhandle;

            self.db.put(key, val, function onDbWriteDone(err) {
                if (err) {
                    cb(xlateDBError(err, 'stat'));
                    return;
                }

                self._no_evict[key] = true;
                self.cache.set(key, stats);
                delete self._no_evict[key];
                cb(null, self.cache.get(key));
            });
        }

        if (!info) {
            self.manta.info(_path, function onInfo(m_err, _info) {
                if (m_err) {
                    cb(xlateMantaError(m_err, 'stat'));
                    return;
                }

                save(_info);
            });
        } else {
            save(info);
        }
    });
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
    var stats;
    var tmp;
    var wstream;

    // Drop this name from the cached parent directory listing
    if (!this.cache.has(k) || !(stats = this.cache.get(k))._cacheFile) {
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
            fs.rename(tmp, stats._cacheFile, function onRename(err) {
                if (err) {
                    log.error({
                        cacheFile: stats._cacheFile,
                        err: err,
                        path: _path,
                        tmpFile: tmp
                    }, '_unstageFromParent: unable to rename tmp file');
                }

                cb(err);
            });
        });
        wstream.end();
    });

    rstream.pipe(lstream);
};


/**
 * Pushes a local cache file to Manta. Note this can happen
 * because a time/size threshold was exceeded or this guy got
 * evicted from the dirty cache.
 *
 * The DB value we load has this:
 *
 * {
 *   local: /path/to/local/cache/file
 *   name: /manta/stor/path,
 *   time: epoch-ms
 * }
 *
 */
MantaFs.prototype._upload = function _upload(key, cb) {
    assert.string(key, 'key');
    assert.optionalFunc(cb, 'callback');

    if (cb)
        cb = once(cb);

    if (this._timers[key]) {
        clearTimeout(this._timers[key]);
        delete this._timers[key];
    }

    var log = this.log;
    var self = this;

    log.debug('_upload(%s): entered', key);

    this.db.get(key, function onDBGet(db_err, val) {
        if (db_err) {
            if (cb) {
                cb(xlateDBError(db_err));
            } else {
                log.error(db_err, '_upload(%s): error fetching from DB', key);
            }
            return;
        }

        log.debug({
            value: val
        }, '_upload(%s): information loaded', key);

        var k = sprintf(FILES_KEY_FMT, val.name);
        self._no_evict[k] = true;
        var rstream = fs.createReadStream(val.local);
        rstream.once('error', function onFileError(err) {
            if (cb) {
                cb(err);
            } else {
                log.error(err, '_upload(%s): error reading from local file %s',
                          key, val.name);
            }
            if (self._no_evict[k])
                delete self._no_evict[k];
        });

        rstream.once('open', function onOpen() {
            self.manta.put(val.name, rstream, function onPut(err) {
                if (err) {
                    if (cb) {
                        cb(xlateMantaError(err));
                    } else {
                        log.error(err,
                                  '_upload(%s): error pushing to manta(%s)',
                                  key, val.name);
                    }
                } else {
                    log.debug('_upload(%s): %s pushed to %s',
                              key, val.local, val.name);

                    self.db.del(key, function onDBDelete(db_err_2) {
                        if (db_err_2) {
                            if (cb) {
                                cb(xlateDBError(db_err_2));
                            } else {
                                log.error(db_err_2,
                                          '_upload(%s): error deleting from DB',
                                          key);
                            }
                        }

                        if (cb) {
                            cb();
                        } else {
                            self.emit('upload', val.name);
                        }
                    });
                }

                if (self._no_evict[k])
                    delete self._no_evict[k];
            });
        });
    });
};


// Waits for the cache file to have bytes up to "pos" (inclusive)
MantaFs.prototype._waitForData = function _waitForData(stats, pos, cb) {
    assert.object(stats, 'stats');
    assert.number(pos, 'position');
    assert.func(cb, 'callback');

    cb = once(cb);

    var k = sprintf(FILES_KEY_FMT, stats._path);
    var self = this;

    function _cb(err) {
        if (self._no_evict[k])
            delete self._no_evict[k];

        cb(err);
    }

    // bump the LRU, and mark this as safe
    this.cache.get(k);
    this._no_evict[k] = true;

    if (stats._pending) {
        // If we have the whole file, or if the current cached file
        // has enough data to satisfy the user's request we're G2G
        function ready(s) {
            return (!stats._pending || s.size === stats.size || s.size >= pos);
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
};



///--- Exports

module.exports = {
    MantaFs: MantaFs,

    createClient: function createClient(opts) {
        assert.object(opts, 'options');

        var _opts = {
            dirty: opts.dirty || 1000,
            dirtyAge: opts.dirtyAge || 60,
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
