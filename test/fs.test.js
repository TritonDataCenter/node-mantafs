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

var FS;
var LOG;
var MANTA;
var M_DATA = 'Hello, MantaFS!';
var M_DIR;
var M_OBJ;
var T_DIR = '/tmp/mantafs.test';


///--- Helpers

function rand(max) {
    return (Math.max(1, Math.floor(Math.random() * max)));
}



///--- Tests

test('setup', function (t) {
    var self = this;

    LOG = bunyan.createLogger({
        name: 'MantaFsTest',
        stream: process.stdout,
        level: process.env.LOG_LEVEL || 'warn',
        src: true,
        serializers: bunyan.stdSerializers
    });

    var test_dir = T_DIR + '/' + libuuid.create() + '/cache';

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

    FS = app.createClient({
        files: parseInt((process.env.FS_CACHE_FILES || 1000), 10),
        log: LOG,
        manta: MANTA,
        path: test_dir,
        sizeMB: parseInt((process.env.FS_CACHE_SIZEMB || 1024), 10),
        ttl: parseInt((process.env.FS_CACHE_TTL || 60), 10)
    });

    FS.on('error', function (err) {
        t.ifError(err);
        self.log.fatal(err, 'MantaFs: Uncaught Error (no cleanup)');
        process.exit(1);
    });

    FS.once('ready', function () {
        M_DIR = '~~/stor/mantafs.test/' + libuuid.create();
        MANTA.mkdirp(M_DIR, function (err) {
            if (err) {
                self.log.fatal(err, 'MantaFs: unable to setup');
                process.exit(1);
            }

            M_OBJ = M_DIR + '/' + libuuid.create();
            var stream = MANTA.createWriteStream(M_OBJ);
            stream.once('close', t.end.bind(t));
            stream.once('error', function (err2) {
                self.log.fatal(err2, 'MantaFs: unable to setup');
                process.exit(1);
            });
            stream.end(M_DATA);
        });
    });
});


// test('stat: directory', function (t) {
//     FS.stat(M_DIR, function (err, stats) {
//         t.ifError(err);
//         t.ok(stats);
//         t.ok(stats instanceof fs.Stats);
//         t.ok(stats.isDirectory());
//         FS.lookup(M_DIR, function (err2, fhandle) {
//             t.ifError(err);
//             t.ok(fhandle);
//             t.equal(typeof (fhandle), 'string');
//             /* JSSTYLED */
//             t.ok(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(fhandle));
//             t.end();
//         });
//     });
// });


// test('readdir: directory', function (t) {
//     FS.readdir(M_DIR, function (err, files) {
//         t.ifError(err);
//         t.ok(files);
//         t.ok(Array.isArray(files));
//         t.ok(files.length);
//         t.end();
//     });
// });


// test('readdir: no entry', function (t) {
//     var dir = M_DIR + '/' + libuuid.create();
//     FS.readdir(dir, function (err, files) {
//         t.ok(err);
//         t.ok(err instanceof app.ErrnoError);
//         t.equal(err.code, 'ENOENT');
//         t.notOk(files);
//         t.end();
//     });
// });


// test('open: 404', function (t) {
//     var obj = M_DIR + '/' + libuuid.create();
//     FS.open(obj, 'r', function (err, files) {
//         t.ok(err);
//         t.ok(err instanceof app.ErrnoError);
//         t.equal(err.code, 'ENOENT');
//         t.notOk(files);
//         t.end();
//     });
// });


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


test('teardown', function (t) {
    FS.shutdown(function (err) {
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
});
