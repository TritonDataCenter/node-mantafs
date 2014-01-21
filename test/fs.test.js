// Copyright 2014 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var crypto = require('crypto');
var fs = require('fs');

var bunyan = require('bunyan');
var manta = require('manta');
var mkdirp = require('mkdirp');
var rimraf = require('rimraf');
var uuid = require('node-uuid');

var test = require('tap').test;

var app = require('../lib');




///--- Globals

var FD;
var FS;
var LOG;
var MANTA;
var M_DATA = 'Hello, MantaFS!';
var M_DIR = '~~/stor/mantafs.test/' + uuid.v4();
var M_SUBDIR_1 = M_DIR +'/subdir';
var M_SUBDIR_2 = M_DIR +'/subdir 2';
var M_404 = M_DIR + '/' + uuid.v4();
var M_OBJ = M_DIR + '/object';
var T_DIR = '/tmp/mantafs.test/' + uuid.v4();


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
        log: LOG,
        manta: MANTA,
        path: '/' + uuid.v4()
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


test('create mantafs infinite space', function (t) {
    var _fs = app.createClient({
        log: LOG,
        manta: MANTA,
        path: T_DIR + '/' + uuid.v4()
    });

    t.ok(_fs);

    _fs.once('error', function (err) {
        t.ifError(err);
        t.end();
    });
    _fs.once('ready', function () {
        _fs.shutdown(function (err) {
            t.ifError(err);
            t.end();
        });
    });
});


test('create mantafs', function (t) {
    FS = app.createClient({
        log: LOG,
        manta: MANTA,
        path: T_DIR + '/cache',
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
            t.end();
        });
    });
}

test('stat: directory', _stat_basic_dir);
test('stat: directory (cached)', _stat_basic_dir);


test('stat: 404', function (t) {
    FS.stat(M_404, function (err, stats) {
        t.ok(err);
        t.equal(err.code, 'ENOENT');
        t.equal(err.syscall, 'stat');
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
        t.equal(err.code, 'ENOENT');
        t.notOk(files);
        t.end();
    });
});


test('readdir: object', function (t) {
    FS.readdir(M_OBJ, function (err, files) {
        t.ok(err);
        t.equal(err.code, 'ENOTDIR');
        t.notOk(files);
        t.end();
    });
});


test('open: 404', function (t) {
    FS.open(M_404, 'r', function (err, files) {
        t.ok(err);
        t.equal(err.code, 'ENOENT');
        t.notOk(files);
        t.end();
    });
});



test('open/read/close: ok', function (t) {
    FS.open(M_OBJ, 'r', function (o_err, fd) {
        t.ifError(o_err);
        if (o_err) {
            t.end();
            return;
        }

        t.ok(fd);

        var sz = Buffer.byteLength(M_DATA);
        var b = new Buffer(sz);
        b.fill(0);
        var len = Math.floor(sz / 3);

        FS.read(fd, b, 0, len, function one(r_err, nbytes) {
            t.ifError(r_err);
            if (r_err) {
                t.end();
                return;
            }

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
        t.equal(err.code, 'EBADF');
        t.end();
    });
});


test('read: bad fd', function (t) {
    FS.read(FD + 100, new Buffer(123), 0, 1, function (err, fd) {
        t.ok(err);
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
    var n = 'i_should_not_exist_yet';
    var d = M_SUBDIR_2 + '/' + n;
    FS.mkdir(d, function (err) {
        t.ifError(err);
        FS.rmdir(d, function (err2) {
            t.ifError(err2);
            FS.readdir(M_SUBDIR_2, function (err3, files) {
                t.ifError(err3);
                t.equal((files || []).indexOf(n), -1);
                t.end();
            });
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
    FS.rmdir(M_SUBDIR_1 + '/' + uuid.v4(), function (err) {
        t.ok(err);
        t.equal(err.code, 'ENOENT');
        t.end();
    });
});


test('rmdir: not dir', function (t) {
    FS.rmdir(M_OBJ, function (err) {
        t.ok(err);
        t.equal(err.code, 'ENOTDIR');
        t.end();
    });
});


test('truncate: ok', function (t) {
    FS.truncate(M_OBJ, 0, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('truncate: ENOENT', function (t) {
    FS.truncate(M_DIR + '/' + uuid.v4(), 0, function (err) {
        t.ok(err);
        if (err)
            t.equal(err.code, 'ENOENT');
        t.end();
    });
});

/*
test('write: start of file', function (t) {
    FS.open(M_OBJ, 'r+', function (o_err, fd) {
        t.ifError(o_err);
        t.ok(fd);
        if (o_err || !fd) {
            t.end();
            return;
        }

        var b = new Buffer('foo');
        FS.write(fd, b, 0, b.length, 1, function (err, written, buf) {
            t.ifError(err);
            t.ok(written);
            t.equal(b.toString(), buf.toString());
            t.equal(written, b.length);
            t.end();
        });
    });
});


test('unlink: object', function (t) {
    FS.unlink(M_OBJ, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('unlink: ENOENT', function (t) {
    FS.unlink(M_SUBDIR_1 + '/' + uuid.v4(), function (err) {
        t.ok(err);
        t.equal(err.code, 'ENOENT');
        t.end();
    });
});


test('write: end of file', function (t) {
    FS.open(M_OBJ, 'a', function (o_err, fd) {
        t.ifError(o_err);
        t.ok(fd);
        if (o_err || !fd) {
            t.end();
            return;
        }

        var b = new Buffer('foo');
        var off = Buffer.byteLength(M_OBJ);
        FS.write(fd, b, 0, b.length, off, function (err, written, buf) {
            t.ifError(err);
            t.ok(written);
            t.equal(written, b.length);
            t.equal(b.toString(), buf.toString());
            t.end();
        });
    });
});


test('ftruncate: ok', function (t) {
    FS.open(M_OBJ, 'r+', function (o_err, fd) {
        t.ifError(o_err);
        t.ok(fd);
        if (o_err || !fd) {
            t.end();
            return;
        }

        FS.ftruncate(fd, 0, function (err) {
            t.ifError(err);
            t.end();
        });
    });
});


test('ftruncate: EBADF', function (t) {
    FS.ftruncate(-1, 0, function (err) {
        t.ok(err);
        t.equal(err.code, 'EBADF');
        t.end();
    });
});




test('write: create file', function (t) {
    FS.open(M_OBJ, 'w', function (o_err, fd) {
        t.ifError(o_err);
        t.ok(fd);
        if (o_err || !fd) {
            t.end();
            return;
        }

        var b = new Buffer('foo');
        FS.write(fd, b, 0, b.length, null, function (w_err, written, buf) {
            t.ifError(w_err);
            t.ok(written);
            t.equal(written, b.length);
            t.equal(b.toString(), (buf || '').toString());
            FS.stat(M_OBJ, function (err, stats) {
                t.ifError(err);
                t.ok(stats);
                stats = stats || {};
                t.equal(stats.size, b.length);
                FS.close(fd, function (err2) {
                    t.ifError(err2);
                    t.end();
                });
            });
        });
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



test('createWriteStream', function (t) {
    var wstream = FS.createWriteStream(M_OBJ);

    t.ok(wstream);

    wstream.once('error', function (err) {
        t.ifError(err);
        t.end();
    });

    wstream.once('close', function () {
        var str = '';
        var rstream = FS.createReadStream(M_OBJ);
        rstream.setEncoding('utf8');
        rstream.on('data', function (chunk) {
            str += chunk;
        });
        rstream.once('end', function () {
            t.equal(str, M_DATA);
            t.end();
        });
    });

    wstream.once('open', function (fd) {
        t.ok(fd);
        wstream.write(M_DATA, 'utf8');
        wstream.end();
    });
});


test('reopen', function (t) {
    FS.shutdown(function (err) {
        t.ifError(err);
        FS = app.createClient({
            files: parseInt((process.env.FS_CACHE_FILES || 1000), 10),
            log: LOG,
            manta: MANTA,
            path: T_DIR + '/cache',
            sizeMB: parseInt((process.env.FS_CACHE_SIZEMB || 1024), 10),
            ttl: parseInt((process.env.FS_CACHE_TTL || 60), 10)
        });

        FS.once('ready', function () {
            FS.readdir(M_DIR, function (err2, files) {
                t.ifError(err2);
                t.ok(files);
                t.equal(files.length, 2);

                var rs = FS.createReadStream(M_OBJ);
                var str = '';
                rs.setEncoding('utf8');
                rs.once('error', function (err4) {
                    t.ifError(err4);
                    t.end();
                });

                rs.on('data', function (chunk) {
                    str += chunk;
                });

                rs.once('end', function () {
                    t.equal(str, M_DATA);
                    t.end();
                });
            });
        });
    });
});
*/

test('teardown', function (t) {
    MANTA.rmr('~~/stor/mantafs.test', function (err) {
        t.ifError(err);
        FS.once('close', function (err2) {
            t.ifError(err2);
            rimraf(T_DIR, function (err3) {
                t.ifError(err3);
                t.end();
            });
        });
        FS.shutdown();
        MANTA.close();
    });
});
