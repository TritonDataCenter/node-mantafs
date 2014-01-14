// Copyright 2014 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var stream = require('stream');
var util = require('util');



///--- API

function DirStream(opts) {
    stream.Transform.call(this, opts);
}
util.inherits(DirStream, stream.Transform);
module.exports = DirStream;


DirStream.prototype._transform = function _transform(chunk, enc, cb) {
    if (chunk) {
        if (Buffer.isBuffer(chunk))
            chunk = chunk.toString('utf8');

        try {
            if (chunk.length)
                this.push(JSON.parse(chunk).name);
        } catch (e) {
            cb(e);
            return;
        }

        cb();
    }
};
