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

var consumer = new MQPullConsumer('RMQ_Con_IOS_2-BENCH', '192.168.1.201:9876;192.168.1.204:9876');
consumer.init();
consumer.start();

consumer.setPullHandler("FANSHOP-BENCH", "CNotify_IOS_2", consumeMessage1);

consumer.pullLoop();

setTimeout(function(){
     consumer.shutdown();
}, 10000000);	//睡眠1秒


function consumeMessage1(_msgs) {
    //操作流程:
    //   直接推送, 无须存数据库操作、无须查找
    //发送失败、重连操作!
    //TODO: deviceToken要是失效/错误? 设备端的检查=>避免消息误发
    var msgs = _msgs.toArraySync();
    logger.debug("Calling consumeMessage of PullConsumer");
    //logger.debug("11" + ConsumeConcurrentlyStatus.RECONSUME_LATER);
    //msg = msgs.get(JInt(0))
    msgs.forEach(function (msg) {
        var topic = msg.getTopicSync();
        var tags = msg.getTagsSync();
        //var bodybytes = new Buffer(msg.getBody());
        //var body = bodybytes.toString(settings.MsgBodyEncoding);  //byte to string
        //var body = msg.getBodySync().toString(settings.MsgBodyEncoding);
        var body = iconv.decode(new Buffer(msg.getBodySync()), settings.MsgBodyEncoding);

        logger.debug(msg.toStringSync());
        // In Python 2.x, bytes is just an alias for str. 所以bytes解码时要注意了, msg.body.decode会出错(bytes没有decode方法)！
        //logger.debug("Message body: " + str(msg.getBody()))
        //logger.debug("Message body: " + str(msg.getBody()).decode(settings.MsgBodyEncoding))
        logger.debug("Message body: " + body);

        if (topic == consumer.topic && tags == consumer.tags) {
            // 执行消费逻辑
            logger.debug("Got message with topic " + topic + " and tags " + tags);
            //由于APN是异步执行pushNotification,每次都得返回消费成功
            //只有在pushNotification本身执行失败的情况下才会返回消费失败

        } else {// 错误的Tag
            logger.error("Error: expecting message with topic " + consumer.topic + " and tags " + consumer.tags
                + ", but got topic " + topic + " and tags " + tags);
            //return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    });
    //return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
}

