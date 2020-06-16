package com.mq.consumer.service;

import com.mq.common.message.Message;
import com.mq.consumer.common.Consumer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/6/3
 */
public class ConsumerServiceImpl implements ConsumerService{
    private Consumer consumer;

    public ConsumerServiceImpl(Consumer consumer) {
        this.consumer=consumer;
    }


    @Override
    public List<Message> sendMessageToConsumer(List<Message> messages,int transSize) {
        ArrayList<Message> reply = new ArrayList<>();
        for (Message message : messages) {
            //这里可以添加一些其他附加信息，暂时不改动了
            reply.add(message);
            System.out.println("收到消息"+message.getUuid());
        }
        //这里可以改成事务
        consumer.addToBuffer(messages);
        consumer.relese(transSize);
        return reply;
    }
}
