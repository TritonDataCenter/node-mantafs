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



///--- Helpers

function rand(max) {
    return (Math.max(1, Math.floor(Math.random() * max)));
}



///--- Tests

before(function (cb) {
    var self = this;

    this.log = bunyan.createLogger({
        name: 'MantaFsTest',
        stream: process.stdout,
        level: process.env.LOG_LEVEL || 'warn',
        src: true,
        serializers: bunyan.stdSerializers
    });

    this.files = parseInt((process.env.FS_CACHE_FILES || 1000), 10);
    this.sizeMB = parseInt((process.env.FS_CACHE_SIZEMB || 1024), 10);
    this.ttl = parseInt((process.env.FS_CACHE_TTL || 60), 10);

    this.test_dir = '/tmp/mantafs/' + libuuid.create();
    this.test_dir_cache = this.test_dir + '/cache';

    this.manta = manta.createClient({
        log: this.log,
        sign: manta.privateKeySigner({
            key: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8'),
            keyId: process.env.MANTA_KEY_ID,
            user: process.env.MANTA_USER
        }),
        user: process.env.MANTA_USER,
        url: process.env.MANTA_URL
    });

    this.fs = app.createClient({
        files: this.files,
        log: this.log,
        manta: this.manta,
        path: this.test_dir_cache,
        sizeMB: this.sizeMB,
        ttl: this.ttl
    });

    this.fs.on('error', function (async_err) {
        self.async_error = async_err;
    });

    this.fs.once('ready', cb);
});


after(function (cb) {
    var self = this;
    this.fs.once('close', function () {
        rimraf('/tmp/mantafs', function (err) {
            cb(err || self.async_error);
            self.manta.close();
        });
    });
    this.fs.close();
});


test('stat: directory', function (t) {
    this.fs.stat('~~/stor', function (err, stats) {
        t.ifError(err);
        t.ok(stats);
        t.ok(stats instanceof fs.Stats);
        t.ok(stats.isDirectory());
        t.end();
    });
});



test('readdir: directory', function (t) {
    this.fs.readdir('~~/stor', function (err, files) {
        t.ifError(err);
        t.ok(files);
        t.ok(Array.isArray(files));
        t.ok(files.length);
        t.end();
    });
});
