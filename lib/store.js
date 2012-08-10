/*jslint node: true, vars: true, indent: 4 */
(function (module) {
    "use strict";

    var MAX_KEYS = 1024 * 1024,
        awssum = require('awssum'),
        amazon = awssum.load('amazon/amazon'),
        S3 = awssum.load('amazon/s3').S3;

    function parseKey(url, content, result) {
        if (!content || !content.Key) {
            return;
        }

        var key = content.Key.substr(url.length),
            matched = key.match(/\/([\d\w]+)(?:\:\:([\d]+))?\.json/);

        if (matched) {
            var id = matched[1],
                version = matched[2] ? parseInt(matched[2]) : 1,
                existing = result[id];

            if (!existing || existing < version) {
                result[id] = version;
            }
        }
    }

    function versionByID(hidden, url, cb) {
        return hidden.s3.ListObjects({
            BucketName:hidden.bucket,
            MaxKeys:MAX_KEYS,
            Prefix:url
        }, function (err, result) {
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
                bucketResult.Contents.forEach(function(content) {
                    return parseKey(url, content, result);
                });
            }
            else {
                return parseKey(url, bucketResult.Contents, result);
            }

            return cb(undefined, result);
        });
    }

    function read(hidden, url, cb) {
        var object = url + ".json";

        return hidden.s3.GetObject({
            BucketName:hidden.bucket,
            ObjectName:object
        }, function (err, result) {
            if (err) {
                return cb(err);
            }

            var body = result.Body.toString(),
                content = JSON.parse(body);

            return cb(undefined, content);
        });
    }

    function connect(properties, cb) {
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

        return cb(undefined, new S3({
            accessKeyId:accessKeyId,
            secretAccessKey:secretAccessKey,
            awsAccountId:awsAccountId,
            region:region
        }));
    }

    module.exports = function (logger, properties, cb) {
        return connect(properties, function (err, s3) {
            if (err) {
                return cb(err);
            }

            var bucket = properties['aws-bucket'];

            if (!bucket) {
                return cb(new Error("store aws-bucket not defined"));
            }

            var hidden = {
                s3:s3,
                bucket:bucket,
                logger:logger
            };

            return cb(undefined, {
                versionByID: function(url, cb) {
                    return versionByID(hidden, url, cb);
                },
                read: function(url, cb) {
                    return read(hidden, url, cb);
                }
            });
        });
    };
})(module);