// Copyright 2013 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var crypto = require('crypto');
var fs = require('fs');

var bunyan = require('bunyan');
var libuuid = require('libuuid');
var manta = require('manta');
var mkdirp = require('mkdirp');
var rimraf = require('rimraf');

var app = require('../lib');

require('nodeunit-plus');



///--- Globals

var FD;
var FS;
var LOG;
var MANTA;
var M_DATA = 'Hello, MantaFS!';
var M_DIR = '~~/stor/mantafs.test/' + libuuid.create();
var M_SUBDIR_1 = M_DIR +'/' + libuuid.create();
var M_SUBDIR_2 = M_DIR +'/' + libuuid.create();
var M_404 = M_DIR + '/' + libuuid.create();
var M_OBJ = M_DIR + '/' + libuuid.create();
var T_DIR = '/tmp/mantafs.test';


///--- Helpers

function rand(max) {
    return (Math.max(1, Math.floor(Math.random() * max)));
}



///--- Tests

test('setup', function (t) {
    LOG = bunyan.createLogger({
        name: 'MantaFsTest',
        stream: (process.env.LOG_LEVEL ?
                 process.stdout : fs.createWriteStream('/dev/null')),
        level: process.env.LOG_LEVEL || 'fatal',
        src: true,
        serializers: bunyan.stdSerializers
    });

    MANTA = manta.createClient({
        log: LOG,
        sign: manta.privateKeySigner({
            key: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8'),
            keyId: process.env.MANTA_KEY_ID,
            user: process.env.MANTA_USER
        }),
        user: process.env.MANTA_USER,
        url: process.env.MANTA_URL
    });

    MANTA.mkdirp(M_SUBDIR_2, function (err) {
        if (err) {
            console.error('MantaFs: unable to setup: %s', err.stack);
            process.exit(1);
        }

        var stream = MANTA.createWriteStream(M_OBJ);
        stream.once('close', t.end.bind(t));
        stream.once('error', function (err2) {
            console.error('MantaFs: unable to setup: %s', err2.stack);
            process.exit(1);
        });
        stream.end(M_DATA);
    });
});


test('create mantafs bad location', function (t) {
    var _fs = app.createClient({
        files: parseInt((process.env.FS_CACHE_FILES || 1000), 10),
        log: LOG,
        manta: MANTA,
        path: '/' + libuuid.create(),
        sizeMB: parseInt((process.env.FS_CACHE_SIZEMB || 1024), 10),
        ttl: parseInt((process.env.FS_CACHE_TTL || 60), 10)
    });

    t.ok(_fs);
    _fs.once('error', function (err) {
        t.ok(err);
        _fs.shutdown(function (err2) {
            t.ifError(err2);
            t.end();
        });
    });
});


test('create mantafs', function (t) {
    FS = app.createClient({
        files: parseInt((process.env.FS_CACHE_FILES || 1000), 10),
        log: LOG,
        manta: MANTA,
        path: T_DIR + '/' + libuuid.create() + '/cache',
        sizeMB: parseInt((process.env.FS_CACHE_SIZEMB || 1024), 10),
        ttl: parseInt((process.env.FS_CACHE_TTL || 60), 10)
    });

    t.ok(FS);
    t.ok(FS.toString());

    FS.on('error', function (err) {
        t.ifError(err);
        LOG.fatal(err, 'MantaFs: Uncaught Error (no cleanup)');
        process.exit(1);
    });

    FS.once('ready', t.end.bind(t));
});


function _stat_basic_dir(t) {
    FS.stat(M_DIR, function (err, stats) {
        t.ifError(err);
        t.ok(stats);
        t.ok(stats instanceof fs.Stats);
        t.ok(stats.isDirectory());
        FS.lookup(M_DIR, function (err2, fhandle) {
            t.ifError(err);
            t.ok(fhandle);
            t.equal(typeof (fhandle), 'string');
            /* JSSTYLED */
            t.ok(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(fhandle));
            t.end();
        });
    });
}

test('stat: directory', _stat_basic_dir);
test('stat: directory (cached)', _stat_basic_dir);


test('stat: 404', function (t) {
    FS.stat(M_404, function (err, stats) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'ENOENT');
        t.notOk(stats);
        t.end();
    });
});


// We want to ensure we exercise all paths of readdir
function _readdir_basic(t) {
    FS.readdir(M_DIR, function (err, files) {
        t.ifError(err);
        t.ok(files);
        t.ok(Array.isArray(files));
        t.ok(files.length);
        t.end();
    });
}
test('readdir: directory', _readdir_basic);
test('readdir: directory (cached)', _readdir_basic);


test('readdir: 404', function (t) {
    FS.readdir(M_404, function (err, files) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'ENOENT');
        t.notOk(files);
        t.end();
    });
});


test('readdir: object', function (t) {
    FS.readdir(M_OBJ, function (err, files) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'ENOTDIR');
        t.notOk(files);
        t.end();
    });
});


test('open: 404', function (t) {
    FS.open(M_404, 'r', function (err, files) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'ENOENT');
        t.notOk(files);
        t.end();
    });
});


test('open/read/close: ok', function (t) {
    FS.open(M_OBJ, 'r', function (o_err, fd) {
        t.ifError(o_err);
        t.ok(fd);

        var sz = Buffer.byteLength(M_DATA);
        var b = new Buffer(sz);
        var len = Math.floor(sz / 3);

        FS.read(fd, b, 0, len, function one(r_err, nbytes) {
            t.ifError(r_err);
            t.equal(nbytes, len);

            // Here we test the second read going to EOF
            FS.read(fd, b, nbytes, sz, function two(r_err2, nbytes2) {
                t.ifError(r_err2);
                t.equal(nbytes2, sz - len);
                t.equal(b.toString(), M_DATA);

                FS.close(fd, function (c_err) {
                    t.ifError(c_err);
                    t.end();
                });
            });
        });
    });
});


test('open: without closing', function (t) {
    FS.open(M_OBJ, 'r', function (err, fd) {
        t.ifError(err);
        t.ok(fd);
        FD = fd;
        t.end();
    });
});


test('close: bogus fd', function (t) {
    FS.close(-1, function (err) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'EBADF');
        t.end();
    });
});


test('read: bad fd', function (t) {
    FS.read(FD + 100, new Buffer(123), 0, 1, function (err, fd) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'EBADF');
        t.notOk(fd);
        t.end();
    });
});


test('mkdir: ok', function (t) {
    FS.mkdir(M_SUBDIR_1, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('read new directory: ok', function (t) {
    FS.readdir(M_SUBDIR_1, function (err, files) {
        t.ifError(err);
        t.ok(files);
        t.notOk(files.length);
        t.end();
    });
});


test('mkdir/rmdir: parent not cached', function (t) {
    var d = M_SUBDIR_2 + '/' + libuuid.create();
    FS.mkdir(d, function (err) {
        t.ifError(err);
        FS.rmdir(d, function (err2) {
            t.ifError(err2);
            t.end();
        });
    });
});


test('rmdir: ok', function (t) {
    FS.rmdir(M_SUBDIR_1, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('rmdir: no entry', function (t) {
    FS.rmdir(M_SUBDIR_1 + '/' + libuuid.create(), function (err) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'ENOENT');
        t.end();
    });
});


test('rmdir: not dir', function (t) {
    FS.rmdir(M_OBJ, function (err) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'ENOTDIR');
        t.end();
    });
});


test('createReadStream', function (t) {
    var opened = false;
    var rstream = FS.createReadStream(M_OBJ);
    var str = '';

    t.ok(rstream);
    rstream.setEncoding('utf8');

    rstream.once('error', function (err) {
        t.ifError(err);
        t.end();
    });

    rstream.on('data', function (chunk) {
        t.ok(chunk);
        str += chunk;
    });

    rstream.once('end', function () {
        t.equal(str, M_DATA);
        t.ok(opened);
        t.end();
    });

    rstream.once('open', function (fd) {
        t.ok(fd);
        opened = true;
    });
});


test('rename: ok', function (t) {
    var name = M_SUBDIR_2 + '/' + libuuid.create();
    FS.rename(M_OBJ, name, function (err) {
        t.ifError(err);
        FS.rename(name, M_OBJ, function (err2) {
            t.ifError(err2);
            t.end();
        });
    });
});


test('rename: directory', function (t) {
    var name = M_SUBDIR_2 + '/' + libuuid.create();
    FS.rename(M_SUBDIR_2, name, function (err) {
        t.ok(err);
        t.equal(err.code, 'EISDIR');
        t.end();
    });
});


test('rename: no parent directory', function (t) {
    var name = M_SUBDIR_1 + '/' + libuuid.create();
    FS.rename(M_SUBDIR_2, name, function (err) {
        t.ok(err);
        t.equal(err.code, 'ENOENT');
        t.end();
    });
});


test('rename: parent not object', function (t) {
    var name = M_OBJ + '/' + libuuid.create();
    FS.rename(M_OBJ, name, function (err) {
        t.ok(err);
        t.equal(err.code, 'ENOTDIR');
        t.end();
    });
});


test('unlink: object', function (t) {
    FS.unlink(M_OBJ, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('unlink: ENOENT', function (t) {
    FS.unlink(M_SUBDIR_1 + '/' + libuuid.create(), function (err) {
        t.ok(err);
        t.ok(err instanceof app.ErrnoError);
        t.equal(err.code, 'ENOENT');
        t.end();
    });
});


test('reopen', function (t) {
    FS.shutdown(function (err) {
        t.ifError(err);
        FS = app.createClient({
            files: parseInt((process.env.FS_CACHE_FILES || 1000), 10),
            log: LOG,
            manta: MANTA,
            path: T_DIR + '/' + libuuid.create() + '/cache',
            sizeMB: parseInt((process.env.FS_CACHE_SIZEMB || 1024), 10),
            ttl: parseInt((process.env.FS_CACHE_TTL || 60), 10)
        });

        FS.once('ready', function () {
            FS.readdir(M_DIR, function (err2, files) {
                t.ifError(err2);
                t.ok(files);
                t.equal(files.length, 1);
                FS.stat(M_DIR + '/' + files[0], function (err3, stats) {
                    t.ifError(err3);
                    t.ok(stats.isDirectory());
                    t.end();
                });
            });
        });
    });
});


test('teardown', function (t) {
    FS.once('close', function (err) {
        t.ifError(err);
        rimraf(T_DIR, function (err2) {
            t.ifError(err2);
            MANTA.rmr(M_DIR, function (err3) {
                t.ifError(err3);
                MANTA.close();
                t.end();
            });
        });
    });

    FS.shutdown();
});
