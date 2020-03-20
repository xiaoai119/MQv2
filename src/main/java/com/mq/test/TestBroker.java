package com.mq.test;

import com.mq.broker.Broker;
import com.mq.common.role.BrokerInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * Created By xfj on 2020/3/16
 */
public class TestBroker {
    public static void main(String[] args) throws UnknownHostException {
        BrokerInfo brokerInfo = new BrokerInfo(InetAddress.getLocalHost().getHostAddress(), 8118,"broker1",new ArrayList<String>());
        Broker broker = new Broker(brokerInfo);
        broker.start();
    }
}
