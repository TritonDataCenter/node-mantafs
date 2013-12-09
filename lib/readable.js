// Copyright 2013 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var stream = require('stream');
var util = require('util');

var assert = require('assert-plus');



///--- Streams

function MantaFsReadStream(opts) {
    assert.object(opts);
    if (opts.encoding !== null)
        assert.optionalString(opts.encoding, 'options.encoding');
    assert.optionalNumber(opts.end, 'options.end');
    assert.optionalNumber(opts.fd, 'options.fd');
    assert.object(opts.fs, 'options.fs');
    assert.optionalNumber(opts.start, 'options.start');
    assert.optionalBool(opts.autoClose, 'options.autoClose');

    stream.Readable.call(this, opts);

    this.autoClose = opts.autoClose === undefined ? true : opts.autoClose;
    this.encoding = opts.encoding || null;

    this._fd = opts.fd;
    this._fs = opts.fs;
    this._end = opts.end || Infinity;
    this._start = opts.start || 0;

    this._pos = this._start;
    this._range = this._end - this._start;

    if (this._fd) {
        setImmediate(this.emit.bind(this, 'open', this._fd));
    } else {
        this._opening = true;
    }
}
util.inherits(MantaFsReadStream, stream.Readable);


MantaFsReadStream.prototype._read = function _read(sz) {
    if (!this._fd && !this._opening) {
        setImmediate(this.emit.bind(this, 'error', new Error('not open')));
        return;
    } else if (this._opening) {
        this.once('open', this._read.bind(this, sz));
        return;
    }

    if (sz > this._range)
        sz = this._range;

    var buf = new Buffer(sz);
    var fd = this._fd;
    var self = this;

    this._fs.read(fd, buf, 0, sz, this._pos, function onRead(err, nbytes) {
        if (err) {
            self.emit('error', err);
        } else {
            self._pos += nbytes;
            self.push(buf.slice(0, nbytes));
            if (nbytes < sz || self._pos >= self._range) {
                self.push(null);
                if (self.autoClose) {
                    self._fs.close(self._fd, function onClose(c_err) {
                        if (c_err) {
                            self._emit('error', c_err);
                        }
                    });
                }
            }
        }
    });
};


MantaFsReadStream.prototype._open = function _open(fd) {
    if (this._fd || this._opening === false)
        throw new Error('stream is already open');

    this._fd = fd;
    this._opening = false;

    setImmediate(this.emit.bind(this, 'open', fd));
};



///--- API

module.exports = {
    MantaFsReadStream: MantaFsReadStream
};
