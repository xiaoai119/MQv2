package com.mq.service;

import com.mq.broker.Broker;
import com.mq.common.message.Message;
import com.mq.common.message.MessageType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/3/16
 */
public class BrokerServiceImpl implements BrokerService {
    Broker broker;

    public BrokerServiceImpl(Broker broker) {
        this.broker=broker;
    }

    public List<Message> sendToBrokerBatch(List<Message> messages){
        ArrayList<Message> result = new ArrayList<>();
        for(Message message:messages){
            Message message1=sendToBroker(message);
            if(message1!=null)result.add(message1);
        }
        return result;
    }

    @Override
    public List<Message> SycSendToBroker(List<Message> messages) {
        return null;
    }

    public Message sendToBroker(Message message){
        //根据Mseeage的类型选择对应的Handler
        if(message.getType()== MessageType.ONE_WAY){
            sendOneWay(message);
            return null;
        }
        if(message.getType()==MessageType.REPLY_EXPECTED) {
            return sendExpectedReply(message);
        }
        if(message.getType()== MessageType.PULL){
            // TODO: 2020/3/16
        }
        return null;
    }

    public void sendOneWay(Message message){
        broker.addToBroker(message);
    }

    public Message sendExpectedReply(Message message){
        return broker.addAndReply(message);
    }
}
