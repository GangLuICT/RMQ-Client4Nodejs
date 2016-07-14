/**
 * Gang Lu created at 2016-6-12
 * E-mail: gang.lu.ict@gmail.com
 *
 * Copyright (c) 2016 bafst.com, All rights reserved.
 */
"use strict";

var settings = require("../settings_MQ");
var logger = settings.logger;

var java = require("java");

// Configure JVM
java.classpath.push(settings.JAVA_EXT_DIRS);
settings.JVM_OPTIONS.forEach(function(opt){
    java.options.push(opt);
});

// Import MQProducer
var MQM = require("../MQMessage");
var MQMessage = MQM.MQMessage;

var MQProducer = require("../MQProducer");

var producer = new MQProducer('MQClient4Python-Producer', 's001:9876;s004:9876');
producer.init(function(){
    producer.start(); // start和shutdown是同步执行的，异常捕捉在外侧进行，封装类里面没有使用try catch
for (var j = 0; j < 1; j++) {
    var MQMsg = new MQMessage('FANSHOP-BENCH',  //topic
        'CNotify_IOS_2',   //tag
        'OrderID001',   //key
        'Hello, RocketMQ!');  //body
    logger.debug("Going to send message: " + MQMsg.tostr());
    for (var i = 0; i < 10; i++) {
        producer.send(MQMsg, function (sendResult) {
            if (sendResult) {
                logger.debug("Message sent result: " + sendResult.toString());
            }
        });
    }
}
});

//producer.shutdown();

//shutdownJVM();
