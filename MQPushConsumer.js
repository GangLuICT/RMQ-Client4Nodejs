/*
* Created by Gang Lu on 6/12/16.
* E-mail: gang.lu.ict@gmail.com
*
* Copyright (c) 2016 bafst.com, All rights reserved.
*/

"use strict";

var settings = require("../settings_MQ");   //配置信息
var logger = settings.logger;
var moment = require('moment'); //时间

var java = require("java");

var DefaultMQPushConsumer= java.import('com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer');
var MQClientException = java.import('com.alibaba.rocketmq.client.exception.MQClientException');
//MessageExt = JPackage('com.alibaba.rocketmq.common.message').MessageExt
//ConsumeConsurrentlyContext = JPackage('com.alibaba.rocketmq.client.consumer.listener').ConsumeConsurrentlyContext
//ConsumeConsurrentlyStatus = JPackage('com.alibaba.rocketmq.client.consumer.listener').ConsumeConsurrentlyStatus
//MessageListenerConcurrently = JPackage('com.alibaba.rocketmq.client.consumer.listener').MessageListenerConcurrently

var MQPushConsumer = function(groupName, namesrvAddr){
    this.consumer = undefined;      //初始化放在了init函数中
    this.groupName = groupName;
    this.namesrvAddr = namesrvAddr;
    this.instanceName = moment().millisecond();  //毫秒值作为instance name
};

//"""批量设置一些基本项(为了尽可能少实现这些API接口,如以后有需要,可以逐个移出init)"""
MQPushConsumer.prototype.init = function () {
    logger.info('Initializing consumer ' + this.instanceName + ' ...');
    this.consumer = DefaultMQPushConsumer(this.groupName);   //创建实例
    this.consumer.setNamesrvAddr(this.namesrvAddr);
    this.consumer.setInstanceName(this.instanceName);
};

MQPushConsumer.prototype.start = function () {
    logger.info('Starting consumer ' + this.instanceName + ' ...');
    this.consumer.start();
};

MQPushConsumer.prototype.shutdown = function () {
    logger.info('Shutting down consumer ' + this.instanceName + ' ...');
    this.consumer.shutdown();
};

MQPushConsumer.prototype.setMessageModel = function (messageModel) {
    logger.info('Setting message model of instance ' + this.instanceName + ' to ' + messageModel.toString());
    //this.consumer.setMessageModel(JObject(messageModel, "com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel"))
    this.consumer.setMessageModel(messageModel);
};

MQPushConsumer.prototype.subscribe = function (topic, subExpression) {
    this.consumer.subscribe(topic, subExpression);
};

MQPushConsumer.prototype.unsubscribe = function (topic) {
    this.consumer.unsubscribe(topic);
};

MQPushConsumer.prototype.setConsumeFromWhere = function (fromwhere) {
    this.consumer.setConsumeFromWhere(fromwhere);
};

MQPushConsumer.prototype.registerMessageListener = function (listener) {
    this.consumer.registerMessageListener(listener);
};