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

var consumer = new MQPushConsumer('MQClient4Python-Consumer', 'jfxr-7:10911;jfxr-6:10911');
consumer.init();

consumer.setMessageModel(MessageModel['CLUSTERING']);   // 默认是CLUSTERING

consumer.subscribe("RMQTopicTest", "TagC");

consumer.setConsumeFromWhere(ConsumeFromWhere['CONSUME_FROM_LAST_OFFSET']);

consumer.registerMessageListener(msgListenerOrderlyProxy);
//consumer.registerMessageListener(msgListenerConcurrentlyProxy);

consumer.start();

setTimeout(function(){

}, 100000);

process.nextTick(function(){
console.log("延迟下一个tick执行");
});
//}

//consumer.shutdown();
