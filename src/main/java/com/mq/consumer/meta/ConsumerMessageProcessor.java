package com.mq.consumer.meta;

import com.alibaba.fastjson.JSON;
import com.mq.broker.service.BrokerService;
import com.mq.common.Topic;
import com.mq.common.message.Message;
import com.mq.common.role.ConsumerMetaInfo;
import com.mq.consumer.allocator.Allocator;
import com.mq.consumer.buffer.Buffer;
import com.mq.producer.net.RPCManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created By xfj on 2020/6/3
 * 用于模拟消费
 */
public class ConsumerMessageProcessor {
    Buffer buffer;
    ConsumerMetaInfo consumerMetaInfo;
    RPCManager rpcManager;
    Allocator allocator;
    int consumeSum=0;

    public ConsumerMessageProcessor(ConsumerMetaInfo consumerMetaInfo, RPCManager rpcManager,Allocator allocator,int bufferSize, double requestThreshold) {
        this.consumerMetaInfo=consumerMetaInfo;
        this.rpcManager=rpcManager;
        this.allocator=allocator;
        buffer=new Buffer(bufferSize,requestThreshold);
    }

    /**
     * 向broker发送push请求
     * 这里可以
     */
    public void sendPushRequestTobroker(){
        //策略是取剩余容量的一半进行预分配
        int remainCapacity = buffer.getRemainCapacity();
        int requestSize=(int)remainCapacity/4;
        if(!buffer.preAllocate(requestSize)||!buffer.canRequest()){
            System.out.println("预分配size"+requestSize+"失败,或buffer达到阈值");
            return;
        }

        Topic topic = allocator.randomAllocate();
        sendPushRequestTobroker(topic,requestSize);
    }

    public void relese(int size){
        buffer.relese(size);
    }

    public void sendPushRequestTobroker(Topic topic,int transSize){
        List<Map.Entry<String, List<Integer>>> entry = topic.getQueues().entrySet().stream().collect(Collectors.toList());
        BrokerService rpcProxy = rpcManager.getRPCProxy(BrokerService.class,entry.get(0).getKey());// 获取远程服务代理
        System.out.println("向broker："+entry.get(0).getKey()+"发送push请求；"+"请求队列为"+ JSON.toJSONString(entry.get(0).getValue())+";size大小为"+transSize);
        rpcProxy.sendPushRequestTobroker(consumerMetaInfo.getName(),topic.getTopicName(),entry.get(0).getValue(),transSize);
    }

    public void timedSendPushRequest() {
        ScheduledExecutorService scheduledSendPushRequest = Executors.newScheduledThreadPool(4);
        SendPushRequestTask task = new SendPushRequestTask();
        scheduledSendPushRequest.scheduleAtFixedRate(task, 0, 500, TimeUnit.MILLISECONDS);
        scheduledSendPushRequest.scheduleAtFixedRate(task, 125, 500, TimeUnit.MILLISECONDS);
        scheduledSendPushRequest.scheduleAtFixedRate(task, 250, 500, TimeUnit.MILLISECONDS);
        scheduledSendPushRequest.scheduleAtFixedRate(task, 375, 500, TimeUnit.MILLISECONDS);
    }

    public void simulateConsumption(int size,int timeUnit){
        ScheduledExecutorService scheduledSendPushRequest = Executors.newScheduledThreadPool(1);
        AddToBufferTask addToBufferTask = new AddToBufferTask(size);
        scheduledSendPushRequest.scheduleAtFixedRate(addToBufferTask, 0, timeUnit, TimeUnit.MILLISECONDS);
    }

    public void doSimulateConsumption(int size){
        List<Message> messageFromBuffer = buffer.getMessageFromBuffer(size);
        for (Message message : messageFromBuffer) {
            System.out.println("consumer消费"+message.getUuid());
            consumeSum++;
            System.out.println("当前已消费消息数"+consumeSum);
        }
    }

    public void addToBuffer(List<Message> messages){
        buffer.addToBuffer(messages);
    }

    class AddToBufferTask implements Runnable {
        int size;

        public AddToBufferTask(int size) {
            this.size = size;
        }

        @Override
        public void run() {
            doSimulateConsumption(size);
        }
    }

    class SendPushRequestTask implements Runnable {
        @Override
        public void run() {
            sendPushRequestTobroker();
        }
    }
}
