package com.mq.broker.service;

import com.mq.common.message.Message;

import java.util.List;

/**
 * Created By xfj on 2020/3/16
 */
public interface BrokerService {
    List<Message> sendToBrokerBatch(List<Message> messages,Integer queueIndex);
    List<Message> SycSendToBroker(List<Message> messages);
}
