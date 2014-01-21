// Copyright 2014 Joyent, Inc.  All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var util = require('util');



///--- Errors

// Ganked from node
// https://github.com/joyent/node/blob/v0.10.22-release/lib/fs.js#L1015-L1023
function ErrnoError(err, syscall, cause) {
    this.cause = cause;
    this.code = err;
    this.message = syscall + ': ' + err +
        (cause ? ': ' + cause.toString() : '');
    this.name = 'ErrnoError';
    this.syscall = syscall;
    Error.captureStackTrace(this, ErrnoError);
}
util.inherits(ErrnoError, Error);



///--- API

// TODO
function xlateDBError(err, syscall) {
    return (err);
}


function xlateMantaError(err, syscall) {
    var code;
    switch (err.name) {
    case 'NotFoundError':
    case 'DirectoryDoesNotExistError':
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

    return (new ErrnoError(code, syscall, err));
}



///--- Exports

module.exports = {
    ErrnoError: ErrnoError,
    xlateDBError: xlateDBError,
    xlateMantaError: xlateMantaError
};
