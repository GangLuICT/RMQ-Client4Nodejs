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
java.options.push(settings.JAVA_EXT_DIRS);

// Import MQProducer
var MQM = require("../MQMessage");
var MQMessage = MQM.MQMessage;

var MQProducer = require("../MQProducer");

var producer = MQProducer('MQClient4Python-Producer', 'jfxr-7:9876;jfxr-6:9876')
producer.init(function(){
    producer.start(function(err){
        var MQMsg = new MQMessage('RMQTopicTest',  //topic
            'TagB',   //tag
            'OrderID001',   //key
            'Hello, RocketMQ!');  //body
        producer.send(MQMsg, function (err) {
            logger.debug("Message sent: " + MQMsg.tostr());
            //顺序方式发送第二条消息
            MQMsg = new MQMessage('RMQTopicTest',  //topic
                'TagC',   //tag
                'OrderID001',   //key
                'Hello, RocketMQ! I am 陆钢');  //body
            producer.send(MQMsg, function (err) {
                logger.debug("Message sent: " + MQMsg.tostr());
            });
        });
    });
});

//producer.shutdown();

//shutdownJVM();