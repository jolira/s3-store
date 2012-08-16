/*jslint node: true, vars: true, indent: 4 */
(function (module) {
    "use strict";

    var MAX_KEYS = 1024 * 1024,
        awssum = require('awssum'),
        amazon = awssum.load('amazon/amazon');

    function stringify(data) {
        try {
            return JSON.stringify(data);
        }
        catch (e) {
            return undefined;
        }
    }

    function parse(data) {
        try {
            return JSON.parse(data);
        }
        catch (e) {
            return data;
        }
    }

    function parseKey(url, content, result) {
        if (!content || !content.Key) {
            return;
        }

        var key = content.Key.substr(url.length),
            matched = key.match(/\/([\w\d\-_]+)\.json/);

        if (matched) {
            var id = matched[1];
            // date = new Date(content.LastModified),
            //version = date.getTime();

            //result[id] = version;
            return result[id] = parse(content.ETag);
        }
    }

    function versionByID(url, cb) {
        var hidden = this,
            start = Date.now();

        return hidden.s3.ListObjects({
            BucketName:hidden.bucket,
            MaxKeys:MAX_KEYS,
            Prefix:url
        }, function (err, result) {
            hidden.logger.info("store s3.ListObjects", url, Date.now() - start);

            if (err) {
                return cb(err);
            }

            var body = result.Body,
                bucketResult = body.ListBucketResult,
                result = {};

            if (bucketResult.IsTruncated !== "false") {
                hidden.logger.error("truncated object list", url);
            }

            if (!bucketResult.Contents) {
                return cb(undefined, result);
            }

            if (Array.isArray(bucketResult.Contents)) {
                bucketResult.Contents.forEach(function (content) {
                    parseKey(url, content, result);
                });
            }
            else {
                parseKey(url, bucketResult.Contents, result);
            }

            return cb(undefined, result);
        });
    }

    function get(url, cb) {
        var hidden = this,
            filename = url + ".json",
            start = Date.now();

        return hidden.s3.GetObject({
            BucketName:hidden.bucket,
            ObjectName:filename
        }, function (err, result) {
            hidden.logger.info("store s3.GetObject", url, Date.now() - start);

            if (err) {
                return cb(err);
            }

            var etag = parse(result.Headers.etag),
                body = result.Body.toString(),
                content = parse(body);

            return cb(undefined, content, etag);
        });
    }

    function read(url, cb) {
        return get.call(this, url, function (err, content, etag) {
            return cb(err, content && content.vals, etag);
        });
    }

    function applyMods(logger, url, data, timestamp, modifications) {
        var keys = Object.keys(modifications),
            mods = data.mods || (data.mods = {}),
            vals = data.vals || (data.vals = {});

        keys.forEach(function (key) {
            var val = modifications[key],
                dbts = mods[key];

            if (timestamp && timestamp < dbts) {
                logger.warning("modification outdated", url, key, mod);
                return;
            }

            mods[key] = timestamp;

            if (val === undefined || val === null) {
                delete vals[key];
            }
            else {
                vals[key] = val;
            }
        });

        return data;
    }

    function save(url, timestamp, modifications, cb) {
        var hidden = this;

        return get.call(hidden, url, function (err, data) {
            if (err && (!err.Body || !err.Body.Error || err.Body.Error.Code !== "NoSuchKey")) {
                return cb(err);
            }

            data = applyMods(hidden.logger, url, data || {}, timestamp, modifications);

            var content = stringify(data),
                filename = url + ".json",
                start = Date.now();

            return hidden.s3.PutObject({
                ObjectName:filename,
                BucketName:hidden.bucket,
                ContentLength:content.length,
                ContentType:"application/json",
                Body:content
            }, function (err, result) {
                hidden.logger.info("store s3.PutObject", url, Date.now() - start);

                if (err) {
                    return cb(err);
                }

                var etag = parse(result.Headers.etag);

                return cb(undefined, data.vals || {}, etag);
            });
        });
    }

    function remove(url, cb) {
        var hidden = this,
            filename = url + ".json",
            start = Date.now();

        return hidden.s3.DeleteObject({
            ObjectName:filename,
            BucketName:hidden.bucket
        }, function (err) {
            hidden.logger.info("store s3.DeleteObject", url, Date.now() - start);

            return cb(err);
        });
    }

    function connect(properties, logger, cb) {
        var accessKeyId = properties["aws-access-key-id"],
            secretAccessKey = properties['aws-secret-access-key'],
            awsAccountId = properties['aws-account-id'],
            region = properties['aws-region'] || amazon.US_WEST_1;

        if (!accessKeyId) {
            return cb(new Error("store aws-access-key-id not defined"));
        }

        if (!secretAccessKey) {
            return cb(new Error("store aws-secret-access-key not defined"));
        }

        var S3 = awssum.load('amazon/s3').S3,
            s3 = new S3({
                accessKeyId:accessKeyId,
                secretAccessKey:secretAccessKey,
                awsAccountId:awsAccountId,
                region:region
            }),
            bucket = properties['aws-store-bucket'];

        if (!bucket) {
            return cb(new Error("store aws-store-bucket not defined"));
        }

        return cb(undefined, {
            s3:s3,
            bucket:bucket,
            logger:logger
        });
    }

    module.exports = function (logger, properties, cb) {
        return connect(properties, logger, function (err, hidden) {
            if (err) {
                return cb(err);
            }

            return cb(undefined, {
                versionByID:function () {
                    return versionByID.apply(hidden, arguments);
                },
                read:function () {
                    return read.apply(hidden, arguments);
                },
                save:function () {
                    return save.apply(hidden, arguments);
                },
                remove:function () {
                    return remove.apply(hidden, arguments);
                }
            });
        });
    };
})(module);