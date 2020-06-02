package com.mq.test1;

import com.mq.broker.common.Broker;
import com.mq.common.Topic;
import com.mq.common.role.BrokerMetaInfo;

import java.util.ArrayList;

/**
 * Created By xfj on 2020/6/2
 */
public class TestBroker1 {
    public static void main(String[] args) {
        BrokerMetaInfo brokerMetaInfo = new BrokerMetaInfo("127.0.0.1", 8119,"broker2",new ArrayList<String>());
        Broker broker = new Broker(brokerMetaInfo);
        Topic topic = new Topic("testTopic1",10);
        Topic topic1 = new Topic("testTopic2",2);

        broker.rigisterTopic(topic);
        broker.rigisterTopic(topic1);

        broker.start();
    }
}
