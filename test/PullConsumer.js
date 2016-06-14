/**
 * Gang Lu created at 2016-6-13
 * E-mail: gang.lu.ict@gmail.com
 *
 * Copyright (c) 2016 bafst.com, All rights reserved.
 */

"use strict";

var iconv = require('iconv-lite');

var settings = require("../settings_MQ");
var logger = settings.logger;

var java = require("java");

// Configure JVM
java.classpath.push(settings.JAVA_EXT_DIRS);
settings.JVM_OPTIONS.forEach(function(opt){
    java.options.push(opt);
});

// Import MQPullConsumer
var MQM = require("../MQMessage");
//var MQMessage = MQM.MQMessage;
var PullStatus = MQM.PullStatus;

var MQPullConsumer = require("../MQPullConsumer");

var consumer = new MQPullConsumer('MQClient4Python-Consumer', 'jfxr-7:9876;jfxr-6:9876');
consumer.init();
consumer.start();

//获取所有消息队列,返回值存储到consumer.mqs
consumer.fetchSubscribeMessageQueues("RMQTopicTest", function(){
    //TODO:
    //    1. fetchSubscribeMessageQueues可能返回异常：Can not find Message Queue for this topic
    //       如果不存在Topic，则创建topic：createTopic(String key, String newTopic, int queueNum)
    //    2. MessageQueue的数目，如何确定的！
    //    3. PullConsumer没有ConsumeFromWhere这项设置
    logger.debug('After fetch subscribe messge queues: ' + consumer.mqs.length);
    //while(true){
        for (var mqid in consumer.mqs) {
        //consumer.mqs.forEach(function(mq){
            var mq = consumer.mqs[mqid];
            logger.debug("Pulling from message queue: " + mq.getQueueIdSync());
            try {
                while(true){
                    // brokerSuspendMaxTimeMillis默认值是20s
                    var pullResult = consumer.pullBlockIfNotFound(mq, '', consumer.getMessageQueueOffset(mq), settings.pullMaxNums);
                    //consumer.pullBlockIfNotFoundAsync(mq, '', consumer.getMessageQueueOffset(mq), settings.pullMaxNums, function (pullResult) {
                        if (pullResult) {
                            consumer.putMessageQueueOffset(mq, pullResult.getNextBeginOffsetSync());
                            var pullStatus = PullStatus[pullResult.getPullStatusSync().toString()];	// JAVA中的enum对应到Python中没有转换为Int，enum对象转换为string的时候是其枚举值的名字，而不是enum的值（0,1...）！
                            if (pullStatus == PullStatus['FOUND']) {
                                logger.debug('Found');
                                logger.debug(pullResult.toString());
                                var msgList = pullResult.getMsgFoundListSync().toArraySync();
                                msgList.forEach(function (msg) {
                                    logger.debug(msg.toString());
                                    //logger.debug("Message body: " + new Buffer(msg.getBodySync()).toString(settings.MsgBodyEncoding));
                                    logger.debug("Message body: " + iconv.decode(new Buffer(msg.getBodySync()), settings.MsgBodyEncoding));
                                    //TODO: 进一步分析pull下来的result
                                });
                            } else if (pullStatus == PullStatus['NO_NEW_MSG']) {
                                logger.debug('NO_NEW_MSG');
                                break;
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
                    //});
                }
            } catch(ex) {
                if (ex.cause)	// Exception from node-java
                    logger.error(ex.cause.getMessageSync());
                else
                    logger.error(ex.name + ': ' + ex.message);
            }
        }//);
    //}
});

//while(true){
//    setTimeout(function(){}, 1000);	//睡眠1秒
//}

//consumer.shutdown();
