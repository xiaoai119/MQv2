package com.mq.test0;

import com.mq.broker.common.Broker;
import com.mq.common.role.BrokerMetaInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * Created By xfj on 2020/3/20
 */
public class TestBroker2 {
    public static void main(String[] args) throws UnknownHostException {
        BrokerMetaInfo brokerMetaInfo = new BrokerMetaInfo(InetAddress.getLocalHost().getHostAddress(), 8119,"broker2",new ArrayList<String>());
        Broker broker = new Broker(brokerMetaInfo);
        broker.start();
    }
}
