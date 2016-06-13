/**
 * Created by Gang Lu on 6/12/16.
 * E-mail: gang.lu.ict@gmail.com
 *
 * Copyright (c) 2016 bafst.com, All rights reserved.
 */
"use strict";

var settings = require("../settings_MQ");   //配置信息
var logger = settings.logger;
var moment = require('moment'); //时间

//需要注意的是node是支持同步/异步的,对于调用java内的函数,要根据情况选择是同步还是异步调用方式!

/**
 * MQProducer
 * @param {String} groupName group name
 * @param {String} namesrvAddr addresses of the name servers
 * @constructor
 */
var MQProducer = function (groupName, namesrvAddr){
    this.producer = null;   //初始化放在了init函数中
    this.groupName = groupName;
    this.namesrvAddr = namesrvAddr;
    this.instanceName = moment().millisecond(); //毫秒值作为instance name
    this.compressMsgBodyOverHowmuch = 4096;     //消息压缩阈值
};

/**
 * init
 * 批量设置一些基本项(为了尽可能少实现这些API接口,如以后有需要,可以逐个移出init)
 * @param {Function} callback the callback function
 */
MQProducer.prototype.init = function(callback) {
    var self = this;

    logger.info('Initializing producer ' + self.instanceName + ' ...');

    self.producer = DefaultMQProducer(self.groupName);   //创建实例
    self.producer.setNamesrvAddr(self.namesrvAddr);
    self.producer.setInstanceName(self.instanceName);
    self.producer.setCompressMsgBodyOverHowmuch(parInt(self.compressMsgBodyOverHowmuch));

    callback && callback();
};

/**
 * start
 * 批量设置一些基本项(为了尽可能少实现这些API接口,如以后有需要,可以逐个移出init)
 * @param {Function} callback the callback function
 */
MQProducer.prototype.start = function (callback) {
    var self = this;

    logger.info('Starting producer ' + self.instanceName + ' ...');
    self.producer.start(function(err) {
        if(err) {
            logger.error('Starting producer failed! Instance name: ' + self.instanceName);
        } else {
            logger.info('Starting producer ' + self.instanceName + ' successfully!');
        }
        callback && callback(err);
    });
};

/**
 * shutdown
 * @param {Function} callback the callback function
 */
MQProducer.prototype.shutdown = function(callback) {
    var self = this;

    logger.info('Shutting down producer ' + self.instanceName + ' ...');
    self.producer.stop(function(err) {
        if(err) {
            logger.error('Stopping producer failed! Instance name: ' + self.instanceName);
        } else {
            logger.info('Stopping producer ' + self.instanceName + ' successfully!');
        }
        callback && callback(err);
    });
};

/**
 * send
 * @param {Object} MQMsg the message to send
 * @param {Function} callback the callback function
 */
MQProducer.prototype.send = function(MQMsg, callback) {
    var self = this;

    logger.debug('Producer ' + self.instanceName + ' sending message: ' + MQMsg.tostr());
    self.producer.send(MQMsg.msg, function(err){
        if(err) {
            logger.error('Stopping producer failed! Instance name: ' + self.instanceName);
        } else {
            logger.info('Stopping producer ' + self.instanceName + ' successfully!');
        }
        callback && callback(err);
    });
};

module.exports = MQProducer;


