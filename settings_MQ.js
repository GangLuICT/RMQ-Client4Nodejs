/**
 * Created by Gang Lu on 6/12/16.
 * E-mail: gang.lu.ict@gmail.com
 *
 * Copyright (c) 2016 bafst.com, All rights reserved.
 */


/*
 * logger
 * By default, winston use npm logging levels: { error: 0, warn: 1, info: 2, verbose: 3, debug: 4, silly: 5 }
 */
var winston = require('winston');
var logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            json: false,
            timestamp: true,
            level: 'error'
        }),
        new winston.transports.File({
            filename: __dirname + '/rmq.log',
            json: false,
            timestamp: true,
            level: 'debug'
        })
    ],
    exceptionHandlers: [
        new (winston.transports.Console)({
            json: false,
            timestamp: true
        }),
        new winston.transports.File({
            filename: __dirname + '/exceptions.log',
            json: false,
            handleExceptions: true,
            humanReadableUnhandledException: true
        })
    ],
    exitOnError: false
});

/*
 * JAVA options
 */
RMQClientJAR = '/home/deploy/rocketmq/alibaba-rocketmq/lib/';
JAVA_EXT_DIRS = RMQClientJAR;
JVM_OPTIONS = '-Xms32m -Xmx256m -mx256m -Xrs';
// the -Xrs flag will “reduce usage of operating-system signals by [the] Java virtual machine (JVM)”, to avoid issues when developing “applications that embed the JVM”

/*
 * RocketMQ configurations
 */
pullMaxNums = 32;
MsgBodyEncoding = 'utf-8';


/*
 * Export the options as the settings struct
 */
var settings = {
    logger: logger,
    JAVA_EXT_DIRS: JAVA_EXT_DIRS,
    JVM_OPTIONS: JVM_OPTIONS,
    pullMaxNums: pullMaxNums,
    MsgBodyEncoding: MsgBodyEncoding
};
module.exports = settings;
