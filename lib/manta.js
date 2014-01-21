// Copyright 2014 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var fs = require('fs');
var path = require('path');
var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');
var manta = require('manta');
var once = require('once');
var uuid = require('node-uuid');

var ErrnoError = require('./errors').ErrnoError;



///--- Globals

var sprintf = util.format;



///--- Helpers

function errno(err, syscall) {
    var code;
    switch (err.name) {
    case 'NotFoundError':
    case 'DirectoryDoesNotExistError':
    case 'ResourceNotFoundError':
        code = 'ENOENT';
        break;

    case 'DirectoryNotEmptyError':
        code = 'ENOTEMPTY';
        break;

    case 'LinkNotObjectError':
        code = 'EISDIR';
        break;

    case 'ParentNotDirectoryError':
        code = 'ENOTDIR';
        break;

    default:
        code = 'EIO';
        break;
    }

    return (new ErrnoError(code, syscall || 'manta', err));
}



///--- API

function MantaClient(opts) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.manta, 'options.manta');

    this.client = opts.manta;
    this.log = opts.log.child({component: 'MantaFsMantaClient'}, true);
}
module.exports = MantaClient;


/**
 * This method simply takes a given Manta path, and creates a local
 * cache file for it.
 */
MantaClient.prototype.cacheObject = function cacheObject(p, opts, cb) {
    assert.string(p, 'path');
    assert.object(opts, 'options');
    assert.string(opts.path, 'options.path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;

    log.trace('cache(%s): entered', p);
    this.client.get(p, function onGet(err, mstream) {
        if (err) {
            log.trace(err, 'cache(%s): failed', p);
            cb(errno(err, opts.syscall));
            return;
        }

        // Ignore errors on cleanup
        var cleanup = fs.unlink.bind(fs, opts.path, function () {
            try {
                mstream.unpipe(fstream);
                mstream.resume();
            } catch (e) {}
        });
        var fstream = fs.createWriteStream(opts.path);

        fstream.once('error', function onFileError(f_err) {
            log.error(f_err, 'cache: error caching %s to %s', p, opts.path);
            cleanup();
            cb(err);
        });

        fstream.once('finish', function onFileFinish() {
            log.trace('cache(%s): object cached to %s', p, opts.path);
            cb();
        });

        fstream.once('open', function onCacheFileOpen() {
            log.trace('cache(%s): caching manta object', p);
        });

        mstream.once('error', function onMantaGetError(m_err) {
            log.warn(m_err, 'cache: error downloading %s', p);
            cleanup();
            cb(err);
        });

        mstream.pipe(fstream);
    });
};


/**
 * Retrieves a Manta directory and stashes it to disk.
 *
 * While slow, callers are expected to read the cached file when this is
 * done.
 *
 * p -> manta path
 * opts.path -> where to write the output to
 * cb -> callback(null)
 */
MantaClient.prototype.cacheDirectory = function cacheDirectory(p, opts, cb) {
    assert.string(p, 'path');
    assert.object(opts, 'options');
    assert.string(opts.path, 'options.path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;
    var self = this;

    log.trace('cacheDir(%s): entered', p);
    var fstream = fs.createWriteStream(opts.path);
    fstream.once('error', cb);
    fstream.once('open', function onFileOpen() {
        self.client.ls(p, function onLsStart(err, res) {
            if (err) {
                cb(errno(err, 'readdir'));
                return;
            }

            res.once('error', function onLsError(err2) {
                cb(errno(err2, 'readdir'));
            });

            res.on('entry', function onLsEntry(e) {
                fstream.write(JSON.stringify(e) + '\n');
            });

            res.once('end', function onLsDone() {
                fstream.once('finish', cb);
                fstream.end();
            });
        });
    });
};


MantaClient.prototype.close = function close() {
    this.client.close();
};


MantaClient.prototype.zero = function zero(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);
    var log = this.log;
    var z = new stream.PassThrough();

    log.trace('zero(%s): entered', p);
    this.client.put(p, z, function onPut(err, res) {
        if (err) {
            log.trace(err, 'zero(%s): failed', p);
            cb(errno(err, 'mzero'));
        } else {
            // Stubbing this out saves a trip to manta
            var info = {
                extension: 'bin',
                type: 'application/octet-stream',
                etag: res.headers.etag,
                parent: path.dirname(p),
                size: 0,
                headers: {
                    'last-modified': res.headers['last-modified']
                }
            };
            log.trace('zero(%s): done => %j', p, info);
            cb(null, info);
        }
    });
    z.end();
};


MantaClient.prototype.mkdir = function mkdir(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);
    var log = this.log;

    log.trace('mkdir(%s): entered', p);
    this.client.mkdir(p, function onMkdir(err, res, info) {
        if (err) {
            log.trace(err, 'mkdir(%s): failed', p);
            cb(errno(err, 'mkdir'));
        } else {
            log.trace('mkdir(%s): done => %j', p, info);
            cb(null, info);
        }
    });
};


/**
 * Performas a Manta HEAD on an object or directory
 *
 * p -> manta path
 * cb -> callback(null, info)
 */
MantaClient.prototype.stat = function stat(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = this.log;

    log.trace('stat(%s): entered', p);

    this.client.info(p, function onInfo(err, info) {
        if (err) {
            log.trace(err, 'stat(%s): failed', p);
            cb(errno(err, 'stat'));
        } else {
            log.trace('stat(%s): done => %j', p, info);
            cb(null, info);
        }
    });
};


/**
 * Performs a Manta Range-GET on an object, and writes the data into
 * a provided buffer.
 *
 * p -> manta path
 * opts.buffer,offset -> output data to here
 * opts.start, opts.end -> use on range GETs (fs.read() does this)
 * cb -> callback(null)
 */
MantaClient.prototype.rangeGet = function rangeGet(p, opts, cb) {
    assert.string(p, 'path');
    assert.object(opts, 'options');
    assert.object(opts.buffer, 'options.buffer');
    assert.number(opts.offset, 'options.offset');
    assert.number(opts.start, 'options.start');
    assert.number(opts.end, 'options.end');
    assert.func(cb, 'callback');

    cb = once(cb);

    var b = opts.buffer;
    var len = opts.end - opts.start;
    var log = this.log;
    var off = opts.offset;
    var _opts = {
        headers: {
            range: sprintf('bytes=%d-%d', opts.start, opts.end)
        }
    };

    log.trace('rangeGet(%s, %d, %d): entered', p, opts.start, opts.end);

    this.client.get(p, _opts, function (err, rstream) {
        if (err) {
            log.trace(err, 'rangeGet(%s): failed', p);
            cb(errno(err, 'read'));
            return;
        }

        var total = 0;
        rstream.once('error', function (err2) {
            log.trace(err2, 'rangeGet(%s): failed', p);
            cb(err);
        });

        rstream.on('data', function (chunk) {
            if (total > len)
                return;

            for (var i = 0; i < chunk.length; i++) {
                if (total <= len) {
                    b[off++] = chunk[i];
                    total++;
                }
            }
        });

        rstream.once('end', function () {
            log.trace('rangeGet(%s): done => %d', p, total);
            cb(null, total);
        });

        rstream.resume();
    });
};


MantaClient.prototype.unlink = function unlink(p, cb) {
    assert.string(p, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);
    var log = this.log;

    log.trace('unlink(%s): entered', p);
    this.client.unlink(p, function onUnlink(err) {
        if (err) {
            log.trace(err, 'unlink(%s): failed', p);
            cb(errno(err, 'unlink'));
        } else {
            log.trace('unlink(%s): done', p);
            cb(null);
        }
    });
};


MantaClient.prototype.toString = function toString() {
    return ('[object MantaFsMantaClient<client=' +
            this.client.toString() + '>]');
};
