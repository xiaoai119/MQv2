package mq.service;

import mq.common.message.Message;

import java.util.List;

/**
 * Created By xfj on 2020/3/16
 */
public interface BrokerService {
    List<Message> sendToBrokerBatch(List<Message> messages);
    List<Message> SycSendToBroker(List<Message> messages);
}
