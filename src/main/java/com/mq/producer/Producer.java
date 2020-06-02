package com.mq.producer;

import com.mq.common.message.Message;
import com.mq.producer.model.MessageProcessor;
import com.mq.producer.model.SendQueueManager;
import com.mq.producer.net.RPCManager;
import com.mq.producer.router.Router;

import java.util.List;

/**
 * Created By xfj on 2020/3/15
 */
public class Producer {
    MessageProcessor messageProcessor;
    private SendQueueManager queueManager;//消息发送队列管理
    private Router router;//消息路由
    private RPCManager rpcManager;//用于获取rpc代理
    private Integer batchSize;


    //默认构造方法
    public Producer() {
        router = new Router();
        queueManager = new SendQueueManager();
        rpcManager = new RPCManager();
        messageProcessor = new MessageProcessor(queueManager,router,rpcManager);
    }

    public void sendBatch(List<Message> messages) {
        messageProcessor.sendBatch(messages);
    }
}
