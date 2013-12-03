// Copyright 2013 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var mkdirp = require('mkdirp');
var once = require('once');
var statvfs = require('statvfs');

var ErrnoError = require('./errors').ErrnoError;



///--- API

function getFsStats(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var p = path.normalize(_path);

    statvfs(p, function onStatVfs(err, stats) {
        if (err) {
            cb(err);
            return;
        }

        var available = stats.bsize * stats.bavail;
        stats.availableMB = Math.floor(available / (1024 * 1024));

        cb(null, stats);
    });
}


function ensureDir(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var p = path.normalize(_path);

    fs.stat(p, function onStat(stat_err, stats) {
        if (stat_err) {
            if (stat_err.code !== 'ENOENT') {
                cb(stat_err);
                return;
            }

            mkdirp(p, 0755, function onMkdirp(err) {
                if (err) {
                    cb(err);
                    return;
                }

                fs.stat(p, cb);
            });

        } else if (!stats.isDirectory) {
            cb(new ErrnoError('ENOTDIR', 'stat'));
        } else {
            cb(null, stats);
        }
    });
}


function ensureAndStat(_path, cb) {
    assert.string(_path, 'path');
    assert.func(cb, 'callback');

    cb = once(cb);

    var p = path.normalize(_path);

    ensureDir(p, function onDirectory(dir_err, dir_stats) {
        if (dir_err) {
            cb(dir_err);
        } else {
            getFsStats(p, function (err, stats) {
                if (err) {
                    cb(err);
                } else {
                    dir_stats.statvfs = stats;
                    cb(null, dir_stats);
                }
            });
        }
    });
}



///--- Exports

module.exports = {
    ensureDir: ensureDir,
    getFsStats: getFsStats,
    ensureAndStat: ensureAndStat
};

    // var log = this.log;
    // var self = this;

    // log.debug('init: entered');

    // this._ensure_cache_dir(this.location, function (dir_err) {
    //     if (dir_err) {
    //         self.emit('error', dir_err);
    //         return;
    //     }

    //     self._get_fs_stats(self.location, function (err, stats) {
    //         if (err) {
    //             self.emit('error', err);
    //             return;
    //         }


    //     function onFsStats(err, stats) {
    //         if (err) {
    //             cb(err);
    //             return;
    //         }

    //         if (stats.availableMB < MB(self.size)) {
    //             self.size = bytes(stats.availableMB);
    //             log.warn('%s has %dMB available. Using as max size',
    //                      self.location, stats.availableMB);
    //         }
    //     });

    //         var keys = self.db.createReadStream();
    //         keys.on('data', function (data) {
    //             self.cache.set(data.key, data.value);
    //         });
    //         keys.once('error', self.emit.bind(self, 'error'));
    //         keys.once('end', self.emit.bind(self, 'ready'));
    //     });
    // });
