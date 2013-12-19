// Copyright 2013 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');



///--- Streams

function WriteStream(opts) {
    assert.object(opts);
    if (opts.encoding !== null)
        assert.string(opts.encoding, 'options.encoding');
    assert.string(opts.flags, 'options.flags');
    assert.number(opts.mode, 'options.mode');
    assert.object(opts.fs, 'options.fs');
    assert.optionalNumber(opts.start, 'options.start');

    stream.Writable.call(this, opts);

    this.bytesWritten = 0;
    this.encoding = opts.encoding;
    this.flags = opts.flags;
    this.mode = opts.mode;
    this.start = opts.start || 0;

    this._fd = opts.fd;
    this._fs = opts.fs;

    this._opening = true;
    this._pos = this.start;

    var self = this;
    this.once('finish', function onFinsh() {
        if (!self._fd)
            return;

        self._fs.close(self._fd, function onClose(err) {
            if (err) {
                self.emit('error', err);
            } else {
                self.emit('close');
            }
        });
    });
}
util.inherits(WriteStream, stream.Writable);


WriteStream.prototype._write = function _write(chunk, encoding, cb) {
    if (!this._fd && !this._opening) {
        cb(new Error('not open'));
        return;
    } else if (this._opening) {
        this.once('open', this._write.bind(this, chunk, encoding, cb));
        return;
    }

    if (!Buffer.isBuffer(chunk))
        chunk = new Buffer(chunk, encoding);

    var fd = this._fd;
    var self = this;

    this._fs.write(fd, chunk, 0, chunk.length, this._pos, function (err, n) {
        if (err) {
            cb(err);
        } else {
            self.bytesWritten += n;
            self._pos += n;
            cb();
        }
    });
};


WriteStream.prototype._open = function _open(fd) {
    if (this._fd || this._opening === false)
        throw new Error('stream is already open');

    this._fd = fd;
    this._opening = false;

    setImmediate(this.emit.bind(this, 'open', fd));
};



///--- API

module.exports = {
    WriteStream: WriteStream
};
