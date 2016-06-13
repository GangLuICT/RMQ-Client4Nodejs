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
java.options.push(settings.JAVA_EXT_DIRS);

// Import MQPullConsumer
var MQM = require("../MQMessage");
//var MQMessage = MQM.MQMessage;
var PullStatus = MQM.PullStatus;

var MQPullConsumer = require("../MQPullConsumer");

var consumer = new MQPullConsumer('MQClient4Python-Consumer', 'jfxr-7:9876;jfxr-6:9876');
consumer.init();
consumer.start();

consumer.fetchSubscribeMessageQueues("RMQTopicTest"); //获取所有消息队列,返回值存储到consumer.mqs
//TODO:
//    1. fetchSubscribeMessageQueues可能返回异常：Can not find Message Queue for this topic
//       如果不存在Topic，则创建topic：createTopic(String key, String newTopic, int queueNum)
//    2. MessageQueue的数目，如何确定的！
//    3. PullConsumer没有ConsumeFromWhere这项设置

while(true){
    consumer.mqs.forEach(function(mq){
        logger.debug("Pulling from message queue: " + str(mq.queueId));
        while(true){
            try {
                var pullResult = consumer.pullBlockIfNotFound(mq, '', consumer.getMessageQueueOffset(mq), settings.pullMaxNums);	// brokerSuspendMaxTimeMillis默认值是20s
                consumer.putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                var pullStatus = PullStatus[str(pullResult.getPullStatus())];	// JAVA中的enum对应到Python中没有转换为Int，enum对象转换为string的时候是其枚举值的名字，而不是enum的值（0,1...）！
                if (pullStatus == PullStatus['FOUND']) {
                    logger.debug('Found');
                    logger.debug(pullResult.toString());
                    var msgList = pullResult.getMsgFoundList();
                    msgList.forEach(function(msg){
                        logger.debug(msg.toString());
                        logger.debug("Message body: " + (new Buffer(msg.getBody())).toString(settings.MsgBodyEncoding));
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
                    logger.error('Wrong pull status: ' + str(pullStatus));
                }
            } catch(ex) {
                logger.error(str(ex.javaClass()) + str(ex.message()));
                logger.error(str(ex.stacktrace()));
            }
        }
    });
}

consumer.shutdown();
