package com.mq.producer.common;

import com.mq.common.message.Message;
import com.mq.producer.meta.ProducerMessageProcessor;
import com.mq.producer.meta.SendQueueManager;
import com.mq.producer.net.RPCManager;
import com.mq.producer.net.TopicWatcher;
import com.mq.producer.router.Router;

import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created By xfj on 2020/3/15
 */
public class Producer {
    ProducerMessageProcessor producerMessageProcessor;
    private SendQueueManager queueManager;//消息发送队列管理
    private Router router;//消息路由
    private RPCManager rpcManager;//用于获取rpc代理
    private Integer batchSize;


    //默认构造方法
    public Producer() {
        router = new Router();
        queueManager = new SendQueueManager();
        rpcManager = new RPCManager();
        producerMessageProcessor = new ProducerMessageProcessor(queueManager,router,rpcManager);
        openTopicWatcher();
    }

    private void openTopicWatcher() {
        Executors.newSingleThreadExecutor().execute(() -> {
            TopicWatcher watcher = new TopicWatcher("/com/mq/topics");
        });
    }


    public void sendBatch(List<Message> messages) {
        producerMessageProcessor.sendBatch(messages);
    }
}
