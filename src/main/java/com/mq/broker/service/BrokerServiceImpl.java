package com.mq.broker.service;

import com.mq.broker.common.Broker;
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

    public List<Message> sendToBrokerBatch(List<Message> messages,Integer queueIndex){
        ArrayList<Message> result = new ArrayList<>();
        for(Message message:messages){
            Message message1=sendToBroker(message,queueIndex);
            if(message1!=null)result.add(message1);
        }
        return result;
    }

    @Override
    public List<Message> SycSendToBroker(List<Message> messages) {
        return null;
    }

    @Override
    public void sendPushRequestTobroker(String consumerName, String topicName, List<Integer> indexes,int transSize){
        System.out.println("收到来自"+consumerName+"的push请求");
        broker.pushMessage(consumerName,indexes,transSize);
    }

    public Message sendToBroker(Message message,Integer queueIndex){
        //根据Mseeage的类型选择对应的Handler
        if(message.getType()== MessageType.ONE_WAY){
            sendOneWay(message,queueIndex);
            return null;
        }
        if(message.getType()==MessageType.REPLY_EXPECTED) {
            return sendExpectedReply(message,queueIndex);
        }
        return null;
    }

    /**
     * 动态push/pull接受consumer的buffer剩余大小
     * @param consumerName consumerName
     * @param size buffer大小
     * @return
     */
    public List<Message> pullMessage(String consumerName,int size){
        return new ArrayList<>();
    }

    public void sendOneWay(Message message,Integer queueIndex){
        broker.addToQueue(message,queueIndex);
    }

    public Message sendExpectedReply(Message message,Integer queueIndex){
        return broker.addAndReply(message,queueIndex);
    }


}
