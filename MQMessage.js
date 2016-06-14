/**
 * Created by Gang Lu on 6/12/16.
 * E-mail: gang.lu.ict@gmail.com
 *
 * Copyright (c) 2016 bafst.com, All rights reserved.
 */
"use strict";

var settings = require("./settings_MQ");   //配置信息
var logger = settings.logger;

var java = require("java");

var Message = java.import('com.alibaba.rocketmq.common.message.Message');
// enum classes:
var PULLSTATUS = java.import('com.alibaba.rocketmq.client.consumer.PullStatus');
var CONSUMEFROMWHERE = java.import('com.alibaba.rocketmq.common.consumer.ConsumeFromWhere');
var CONSUMECONCURRENTLYSTATUS = java.import('com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus');
var CONSUMEORDERLYSTATUS = java.import('com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus');
var MESSAGEMODEL = java.import('com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel');


// Class MQMessage
var MQMessage = function(topic, tags, keys, body){
    this.topic = topic;
    this.tags = tags;
    this.keys = keys;
    this.body = body;
    var byteArray = java.newArray("byte", this.body.split('').map(function(c) { return java.newByte(String.prototype.charCodeAt(c)); }));	//生成byte array的固定方式！
    //var byteArray = new Buffer(this.body, settings.MsgBodyEncoding);	//Buffer方式不work
    //sync methods:
    this.msg = new Message(this.topic, this.tags, this.keys, byteArray); //string to bytes
    //this.msg = java.newInstanceSync("com.alibaba.rocketmq.common.message.Message", this.topic, this.tags, this.keys, byteArray); //string to bytes
    //async method: java.newInstance(className, [args...], callback);
};

MQMessage.prototype.tostr = function(){
    return this.topic + "::" + this.tags + "::" + this.keys + "::" + this.body;
};

exports.MQMessage = MQMessage;

// enum classes

// PullResult的返回结果
var PullStatus = {
    //'FOUND': 0,  // Founded
    'FOUND': PULLSTATUS.FOUND,
    //'NO_NEW_MSG': 1,  // No new message can be pull
    'NO_NEW_MSG': PULLSTATUS.NO_NEW_MSG,
    //'NO_MATCHED_MSG': 2,  // Filtering results can not match
    'NO_MATCHED_MSG': PULLSTATUS.NO_MATCHED_MSG,
    //'OFFSET_ILLEGAL': 3   // Illegal offset，may be too big or too small
    'OFFSET_ILLEGAL': PULLSTATUS.OFFSET_ILLEGAL
};
exports.PullStatus = PullStatus;

// PushConsumer消费时选择第一次订阅时的消费位置
var ConsumeFromWhere = {
    // 一个新的订阅组第一次启动从队列的最后位置开始消费
    // 后续再启动接着上次消费的进度开始消费
    //'CONSUME_FROM_LAST_OFFSET': 0,
    'CONSUME_FROM_LAST_OFFSET': CONSUMEFROMWHERE.CONSUME_FROM_LAST_OFFSET,
    //@Deprecated
    //'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST': 1,
    'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST': CONSUMEFROMWHERE.CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    //@Deprecated
    //'CONSUME_FROM_MIN_OFFSET': 2,
    'CONSUME_FROM_MIN_OFFSET': CONSUMEFROMWHERE.CONSUME_FROM_MIN_OFFSET,
    //@Deprecated
    //'CONSUME_FROM_MAX_OFFSET': 3,
    'CONSUME_FROM_MAX_OFFSET': CONSUMEFROMWHERE.CONSUME_FROM_MAX_OFFSET,
    // 一个新的订阅组第一次启动从队列的最前位置开始消费<br>
    // 后续再启动接着上次消费的进度开始消费
    //'CONSUME_FROM_FIRST_OFFSET': 4,
    'CONSUME_FROM_FIRST_OFFSET': CONSUMEFROMWHERE.CONSUME_FROM_FIRST_OFFSET,
    // 一个新的订阅组第一次启动从指定时间点开始消费,时间点设置参见DefaultMQPushConsumer.consumeTimestamp参数
    // 后续再启动接着上次消费的进度开始消费
    //'CONSUME_FROM_TIMESTAMP': 5,
    'CONSUME_FROM_TIMESTAMP': CONSUMEFROMWHERE.CONSUME_FROM_TIMESTAMP
};
exports.ConsumeFromWhere = ConsumeFromWhere;

// PushConsumer消费后的返回值(并发消费时)
var ConsumeConcurrentlyStatus = {
    //'CONSUME_SUCCESS': 0,  // Success consumption
    'CONSUME_SUCCESS': CONSUMECONCURRENTLYSTATUS.CONSUME_SUCCESS,
    //'RECONSUME_LATER': 1,  // Failure consumption,later try to consume
    'RECONSUME_LATER': CONSUMECONCURRENTLYSTATUS.RECONSUME_LATER
};
exports.ConsumeFromWhere = ConsumeFromWhere;

// PushConsumer消费后的返回值(顺序消费时)
var ConsumeOrderlyStatus ={
    //'SUCCESS': 0,  // Success consumption
    'SUCCESS': CONSUMEORDERLYSTATUS.SUCCESS,
    //'ROLLBACK': 1,  // Rollback consumption(only for binlog consumption)
    'ROLLBACK': CONSUMEORDERLYSTATUS.ROLLBACK,
    //'COMMIT': 2,  // Commit offset(only for binlog consumption)
    'COMMIT': CONSUMEORDERLYSTATUS.COMMIT,
    //'SUSPEND_CURRENT_QUEUE_A_MOMENT': 3   // Suspend current queue a moment
    'SUSPEND_CURRENT_QUEUE_A_MOMENT': CONSUMEORDERLYSTATUS.SUSPEND_CURRENT_QUEUE_A_MOMENT
};
exports.ConsumeOrderlyStatus = ConsumeOrderlyStatus;

// PushConsumer的消息model
var MessageModel = {
    //'BROADCASTING': 0,  // broadcast
    'BROADCASTING': MESSAGEMODEL.BROADCASTING,
    //'CLUSTERING': 1     // clustering
    'CLUSTERING': MESSAGEMODEL.CLUSTERING
};
exports.MessageModel = MessageModel;
