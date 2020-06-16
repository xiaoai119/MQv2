package com.mq.broker.meta;

import com.mq.broker.cursor.Cursor;
import com.mq.broker.cursor.QueueCache;
import com.mq.broker.net.RPCManager;
import com.mq.common.message.Message;
import com.mq.common.message.MessageType;
import com.mq.consumer.service.ConsumerService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created By xfj on 2020/6/10
 */
public class BrokerMessageProcessor {
    private QueueCache queueCache;
    private QueueManager queueManager;
    private Cursor cursor;
    private int maxBatch;
    private RPCManager rpcManager;

    public BrokerMessageProcessor(QueueCache queueCache, QueueManager queueManager, Cursor cursor, int maxBatch, RPCManager rpcManager) {
        this.queueCache = queueCache;
        this.queueManager = queueManager;
        this.cursor = cursor;
        this.maxBatch = maxBatch;
        this.rpcManager = rpcManager;
    }

    public boolean addToQueue(Message message, Integer queueIndex){
        //根据queueIndex获取队列
        MyQueue queue = queueManager.getQueue(queueIndex);
        queue.putAtHeader(message);
        System.out.println("向队列"+queueIndex+"添加消息"+message.getUuid());
        System.out.println("队列"+queueIndex+"size为"+queue.size());
        return true;
    }

    public Message addAndReply(Message message,Integer queueIndex){
        if(addToQueue(message,queueIndex)){
            Message ack = new Message("ACK", MessageType.ACK, message.getNum());
            ack.setUuid(message.getUuid());
            return ack;
        }
        return null;
    }

    public void pushMessage(String consumerName, List<Integer> indexes, int transSize){
        HashMap<Integer, List<Message>> messagesToSend = cursor.getMessagesToSend(indexes, transSize);
        ArrayList<Message> messagesToSendList = new ArrayList<>();
        for (Map.Entry<Integer, List<Message>> entry : messagesToSend.entrySet()) {
            messagesToSendList.addAll(entry.getValue());
            for (Message message : entry.getValue()) {
                //向cache中添加待发送消息
                queueCache.add(entry.getKey(),message);
            }
            doPushMessages(consumerName, messagesToSendList,transSize);
        }
    }

    private void doPushMessages(String consumerName, ArrayList<Message> messagesToSendList,int transSize) {
        ConsumerService rpcProxy = rpcManager.getRPCProxy(ConsumerService.class, consumerName);// 获取远程服务代理
        rpcProxy.sendMessageToConsumer(messagesToSendList,transSize);
    }

}
