/*jslint white: true, forin: false, node: true, indent: 4 */
(function (module) {
    "use strict";

    /*
     * for this test to be successful, you will have to have a configuration file set up
     * containing valid store entries, such as:
     * {
     *   "aws-access-key-id" : "0FL2BˆCEW5XED0T3VJG2",
     *   "aws-secret-access-key" : "aP14UHZLYyMRx99QiMXnoGAmU2kDBabHgZWnFZ06",
     *   "aws-account-id" : "8913-7199-8811",
     *   "aws-bucket" : "hubz",
     *   "aws-region": "us-west-1"
     * }
     */
    var MockS3 = function () {
            var putCount = 0,
                getCount = 0;

            this.PutObject = function (arg, cb) {
                switch (putCount++) {
                    case 0:
                        return cb(undefined, {
                            Headers:{
                                etag:"\"c471a30c5517c593dbc2dc310f6bfbc5\""
                            }
                        });
                    case 1:
                        return cb(undefined, {
                            Headers:{
                                etag:"\"3c1eb0c39a16516e57b57b3394c2f7f5\""
                            }
                        });
                }
            };
            this.GetObject = function (arg, cb) {
                switch (getCount++) {
                    case 0:
                        return cb(undefined, {
                            Headers:{
                                etag:"c471a30c5517c593dbc2dc310f6bfbc5"
                            },
                            Body:"{\"vals\":{\"a\":1,\"b\":2,\"c\":3,\"d\":4},\"mods\":{}}"
                        });
                    case 1:
                        return cb(undefined, {
                            Headers:{
                                etag:"\"3c1eb0c39a16516e57b57b3394c2f7f5\""
                            },
                            Body:"{\"vals\":{\"a\":111,\"c\":3,\"d\":4},\"mods\":{\"a\":1345007357931,\"b\":1345007357931}}"
                        });
                }
            };
            this.DeleteObject = function (arg, cb) {
                return cb();
            }
        },
        OBJ = {a:1, b:2, c:3, d:4 },
        URL = "test/1",
        now = Date.now(),
        loader = require('server-config'),
        assert = require('assert'),
        vows = require('vows'),
        horaa = require('horaa'),
        store = require('../lib/store'),
        removeTest = {
            topic:function (db, data, etag) {
                db.remove(URL, this.callback);
            },
            "make sure there is no error":function (err) {
                assert.isUndefined(err);
            }
        },
        readTest = {
            topic:function (db, data, etag) {
                var self = this;

                db.read(URL, function (err, data, etag) {
                    self.callback(err, db, data, etag);
                });
            },
            "make sure there is no error and we got data back":function (err, db, data, etag) {
                assert.isNull(err);
                assert.isString(etag);
                assert.deepEqual({ a:111, d:4, c:3 }, data);
            },
            "delete":removeTest
        },
        updateTest = {
            topic:function (db, data, etag) {
                var self = this;

                db.update(URL, Date.now(), {
                    a:111,
                    b:null
                }, function (err, data, etag) {
                    self.callback(err, db, data, etag);
                });
            },
            "make sure there is no error and we got data back":function (err, db, data, etag) {
                assert.isNull(err);
                assert.isString(etag);
                assert.deepEqual({ a:111, d:4, c:3 }, data);
            },
            "read":readTest
        },
        createTest = {
            "topic":function (db) {
                var self = this;

                db.create(URL, OBJ, function (err, data, etag) {
                    self.callback(err, db, data, etag);
                });
            },
            "make sure there is no error and we got data back":function (err, db, data, etag) {
                assert.isNull(err);
                assert.isString(etag);
                assert.deepEqual(OBJ, data);
            },
            "update":updateTest
        },
        openTest = {
            "topic":function (config) {
                if (config["aws-access-key-id"] === "aws-access-key-id" || config["aws-secret-access-key"] === "aws-secret-access-key") {
                    var awssum = horaa('awssum');

                    awssum.hijack('load', function (arg) {
                        assert.equal(arg, 'amazon/s3');

                        return {
                            S3:MockS3
                        };
                    });
                }

                return store({
                    info:function () {
                        console.log.apply(console.log, arguments);
                    }
                }, config, this.callback);
            },
            "create a record":createTest
        };

    // Create a Test Suite
    vows.describe('store').addBatch({
        'load the configuration':{
            topic:function () {
                loader({
                    "aws-region":"aws-region",
                    "aws-account-id":"aws-account-id",
                    "aws-access-key-id":"aws-access-key-id",
                    "aws-secret-access-key":"aws-secret-access-key",
                    "aws-store-bucket":"aws-store-bucket",
                    "application-name":"test"
                }, "~/.site-manager.json", this.callback);
            },
            'create the store object':openTest
        }
    }).export(module);
})(module);
