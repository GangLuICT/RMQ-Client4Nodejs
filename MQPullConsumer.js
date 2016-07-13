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

var PullStatus = MQM.PullStatus;

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

MQPullConsumer.prototype.setBrokerSuspendMaxTimeMillis = function (brokerSuspendMaxTimeMillis, callback) {
    this.consumer.setBrokerSuspendMaxTimeMillisSync(brokerSuspendMaxTimeMillis);
    callback && callback();
};

MQPullConsumer.prototype.setConsumerTimeoutMillisWhenSuspend = function (consumerTimeoutMillisWhenSuspend, callback) {
    this.consumer.setConsumerTimeoutMillisWhenSuspendSync(consumerTimeoutMillisWhenSuspend);
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

MQPullConsumer.prototype.setPullHandler = function (topic, tags, consumeMessage){
    this.topic = topic;
    this.tags = tags;
    this.consumeMessage = consumeMessage;
};

//开始循环拉取消息
MQPullConsumer.prototype.pullLoop = function(){
    var self = this;
    //获取所有消息队列,返回值存储到consumer.mqs
    self.fetchSubscribeMessageQueues(this.topic, function(){
        //TODO:
        //    1. fetchSubscribeMessageQueues可能返回异常：Can not find Message Queue for this topic
        //       如果不存在Topic，则创建topic：createTopic(String key, String newTopic, int queueNum)
        //    2. MessageQueue的数目，如何确定的！
        //    3. PullConsumer没有ConsumeFromWhere这项设置
        logger.debug('After fetch subscribe messge queues: ' + self.mqs.length);
        for (var mqid in self.mqs) {
            //consumer.mqs.forEach(function(mq){
            var mq = self.mqs[mqid];
            logger.debug("Pulling from message queue: " + mq.getQueueIdSync());
            try {
                pullMessagesAsync(self, mq);
            } catch (ex) {
                if (ex.cause)	// Exception from node-java
                    logger.error(ex.cause.getMessageSync());
                else
                    logger.error(ex.name + ': ' + ex.message);
            }
        }
    });
};

function pullMessagesAsync(consumer, mq) {
    //var pullResult = self.pullBlockIfNotFound(mq, '', self.getMessageQueueOffset(mq), settings.pullMaxNums);
    consumer.pullBlockIfNotFoundAsync(mq, this.topic, consumer.getMessageQueueOffset(mq), settings.pullMaxNums, function (pullResult) {
        pullMessagesAsync(consumer, mq);    //继续异步调用、拉取消息
        if (pullResult) {
            consumer.putMessageQueueOffset(mq, pullResult.getNextBeginOffsetSync());
            var pullStatus = PullStatus[pullResult.getPullStatusSync().toString()];	// JAVA中的enum对应到Python中没有转换为Int，enum对象转换为string的时候是其枚举值的名字，而不是enum的值（0,1...）！
            if (pullStatus == PullStatus['FOUND']) {
                logger.debug('Found');
                logger.debug(pullResult.toString());
                var msgList = pullResult.getMsgFoundListSync();
                this.consumeMessage(msgList);
            } else if (pullStatus == PullStatus['NO_NEW_MSG']) {
                logger.debug('NO_NEW_MSG');
            } else if (pullStatus == PullStatus['NO_MATCHED_MSG']) {
                logger.debug('NO_MATCHED_MSG');
            } else if (pullStatus == PullStatus['OFFSET_ILLEGAL']) {
                logger.debug('OFFSET_ILLEGAL');
            } else {
                logger.error('Wrong pull status: ' + pullStatus.toString());
            }
        } else {
            logger.debug('This pulling does not get any result!');
        }
    });
}

module.exports = MQPullConsumer;
