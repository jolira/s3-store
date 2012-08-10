/*jslint node: true, vars: true, indent: 4 */
(function (module) {
    "use strict";

    var awssum = require('awssum'),
        amazon = awssum.load('amazon/amazon'),
        S3 = awssum.load('amazon/s3').S3;

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
                bucket:bucket
            };

            return cb(undefined, {

            });
        });
    };
})(module);