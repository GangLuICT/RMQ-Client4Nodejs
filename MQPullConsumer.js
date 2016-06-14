/**
 * Created by Gang Lu on 6/13/16.
 * E-mail: gang.lu.ict@gmail.com
 *
 * Copyright (c) 2016 bafst.com, All rights reserved.
 */
"use strict";

var settings = require("./settings_MQ");   //配置信息
var logger = settings.logger;
var moment = require('moment'); //时间

var java = require("java");

var DefaultMQPullConsumer= java.import('com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer');
//var MQClientException = java.import('com.alibaba.rocketmq.client.exception.MQClientException');
//var PullResult = java.import('com.alibaba.rocketmq.client.consumer.PullResult');
//var MessageQueue = java.import('com.alibaba.rocketmq.common.message.MessageQueue');

var MQPullConsumer = function(groupName, namesrvAddr) {
    this.consumer = undefined;    //初始化放在了init函数中
    this.groupName = groupName;
    this.namesrvAddr = namesrvAddr;
    this.instanceName = moment().format("x");  //毫秒值作为instance name，默认返回string

    this.mqs = undefined;
    this.offseTable = {};    // map of message queue id to queue offset

};

//"""批量设置一些基本项(为了尽可能少实现这些API接口,如以后有需要,可以逐个移出init)"""
MQPullConsumer.prototype.init = function () {
    logger.info('Initializing consumer ' + this.instanceName + ' ...');
    this.consumer = new DefaultMQPullConsumer(this.groupName);   //创建实例
    this.consumer.setNamesrvAddrSync(this.namesrvAddr);
    this.consumer.setInstanceNameSync(this.instanceName);
};

//重要的函数都是用同步的形式

MQPullConsumer.prototype.start = function () {
    logger.info('Starting consumer ' + this.instanceName + ' ...');
    this.consumer.startSync();  //sync
};

MQPullConsumer.prototype.shutdown = function () {
    logger.info('Shutting down consumer ' + this.instanceName + ' ...');
    this.consumer.shutdownSync();   //sync
};

//同步的方式调用,外部函数名称还是叫pullBlockIfNotFound
//调用函数后加Sync
MQPullConsumer.prototype.pullBlockIfNotFound = function (mq, subExpression, offset, maxNums) {
    var pullResult = this.consumer.pullBlockIfNotFoundSync(mq, subExpression, this.getMessageQueueOffset(mq), maxNums);
    //callback && callback(pullResult);
    return pullResult;
};

//异步的方式调用,外部函数名称改成了pullBlockIfNotFoundAsync
MQPullConsumer.prototype.pullBlockIfNotFoundAsync = function (mq, subExpression, offset, maxNums, callback) {
    this.consumer.pullBlockIfNotFound(mq, subExpression, this.getMessageQueueOffset(mq), maxNums, function(err, result){
        if (err) {
            logger.error('Some err occurs when pulling messages. Please look up the exceptions reported!');
            callback && callback(undefined);
        } else {
            callback && callback(result);
        }
    });
};

MQPullConsumer.prototype.fetchSubscribeMessageQueues = function (topic, callback) {
    this.mqs = this.consumer.fetchSubscribeMessageQueuesSync(topic).toArraySync();
    callback && callback();
};

//获取某个MQ中的当前消息的offset
MQPullConsumer.prototype.getMessageQueueOffset = function (mq) {
    var haskey = this.offseTable[mq.getQueueIdSync()];
    if (haskey === undefined)
        return 0;
    else
        return haskey;
};

//设置某个MQ中的当前消息的offset(更新后的值)
MQPullConsumer.prototype.putMessageQueueOffset = function (mq, offset) {
    this.offseTable[mq.getQueueIdSync()] = offset;
};

module.exports = MQPullConsumer;
