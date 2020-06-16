package com.mq.consumer.service;

import com.mq.common.message.Message;

import java.util.List;

/**
 * Created By xfj on 2020/6/3
 */
public interface ConsumerService {
    public List<Message> sendMessageToConsumer(List<Message> messages,int transSize);
}
