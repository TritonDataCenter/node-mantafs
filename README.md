# mantafs

`mantafs` provides an API for interacting with
[Joyent Manta](http://www.joyent.com/products/manta) that is drop-in compatible
with node's [fs](http://nodejs.org/api/fs.html) API. This API automatically
manages interactions with Manta by maintaining a local on-disk cache; this will
give you a dramatic performance increase (since in most cases the WAN is
avoided), at the cost of no longer being consistent at any point in time with
your data in Manta (i.e., this is 'A' in CAP).

# Usage

As always:

    npm install mantafs

Then to use:

```javascript
var assert = require('assert');
var manta = require('manta');
var mantafs = require('mantafs');

var fs = mantafs.createClient({
    files: 10000,                // how many local files to keep
    path: '/var/tmp/mantafs',    // where to stash cached files
    sizeMB: 1000,                // how much disk space to consume
    ttl: 3600,                   // if no cache-control header, the default TTL
    manta: manta.createClient({  // must provide a Manta client
        sign: manta.privateKeySigner({
            key: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8'),
            keyId: process.env.MANTA_KEY_ID,
            user: process.env.MANTA_USER
        }),
        user: process.env.MANTA_USER,
        url: process.env.MANTA_URL
    })
});


fs.once('ready', function () {
    fs.readdir('~/store', function (err, files) {
        assert.ifError(err);

        console.log(files);

        fs.close(function (err2) {
            assert.ifError(err2);
            manta.close();
        });
    });
});

```
