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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
//        HashMap<Integer, List<Message>> messagesToSend = cursor.getMessagesToSend(indexes, transSize,consumerName);
        HashMap<Integer, List<Message>> messagesToSend = cursor.getMessagesToSend(indexes, transSize);
        ArrayList<Message> messagesToSendList = new ArrayList<>();
        for (Map.Entry<Integer, List<Message>> entry : messagesToSend.entrySet()) {
            messagesToSendList.addAll(entry.getValue());
            for (Message message : entry.getValue()) {
                //向cache中添加待发送消息
                queueCache.add(entry.getKey(),message);
            }
        }
        List<Message> messages = doPushMessages(consumerName, messagesToSendList, transSize);
        List<String> keys = messages.stream().map(message -> message.getUuid()).collect(Collectors.toList());
//        deleteFrpmCache(messagesToSend, keys);
    }


    private void deleteFrpmCache(HashMap<Integer, List<Message>> messagesToSend, List<String> keys) {
        for (Map.Entry<Integer, List<Message>> entry : messagesToSend.entrySet()) {
            for (Message message : entry.getValue()) {
                //向cache中添加待发送消息
                if(keys.contains(message.getUuid()))
                queueCache.remove(message.getUuid(),entry.getKey());
            }
        }
    }

    private List<Message> doPushMessages(String consumerName, ArrayList<Message> messagesToSendList,int transSize) {
        ConsumerService rpcProxy = rpcManager.getRPCProxy(ConsumerService.class, consumerName);// 获取远程服务代理
        return rpcProxy.sendMessageToConsumer(messagesToSendList, transSize);
    }

    public void reSendMessage(int outTime,int executeTime){
        ReSendMessageTask reSendMessageTask = new ReSendMessageTask(outTime);
        ScheduledExecutorService scheduledReSendMessage = Executors.newScheduledThreadPool(1);
        scheduledReSendMessage.scheduleAtFixedRate(reSendMessageTask, 0, executeTime, TimeUnit.MILLISECONDS);
    }

    class ReSendMessageTask implements Runnable {
        int outTime;

        public ReSendMessageTask(int outTime) {
            this.outTime = outTime;
        }

        @Override
        public void run() {
            ConcurrentHashMap<Integer, List<Message>> outTimeMessage = queueCache.getOutTimeMessage(outTime);
            for (Map.Entry<Integer, List<Message>> entry : outTimeMessage.entrySet()) {
                if(!entry.getValue().isEmpty()){
                    MyQueue queue = queueManager.getQueue(entry.getKey());
                    queue.addAll(entry.getValue());
                    System.out.println("重新发送"+entry.getValue().size()+"条消息");
                }
            }
        }
    }

}
