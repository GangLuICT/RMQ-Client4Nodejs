/**
 * Gang Lu created at 2016-6-13
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

// Import MQPushConsumer
var MQM = require("../MQMessage");
//var MQMessage = MQM.MQMessage;
var ConsumeFromWhere = MQM.ConsumeFromWhere;
var MessageModel = MQM.MessageModel;
var MQML = require("../MQMessageListener");
var msgListenerConcurrentlyProxy = MQML.msgListenerConcurrentlyProxy;
var msgListenerOrderlyProxy = MQML.msgListenerOrderlyProxy;

var MQPushConsumer = require("../MQPushConsumer");

var consumer = new MQPushConsumer('MQClient4Python-Consumer', 'jfxr-7:9876;jfxr-6:9876');
consumer.init();

consumer.setMessageModel(MessageModel['CLUSTERING']);   // 默认是CLUSTERING

consumer.subscribe("RMQTopicTest", "TagB");

consumer.setConsumeFromWhere(ConsumeFromWhere['CONSUME_FROM_LAST_OFFSET']);

consumer.registerMessageListener(msgListenerOrderlyProxy);

consumer.start();

while(true){
    setTimeout(function(){}, 1000);	//睡眠1秒
}

consumer.shutdown();
