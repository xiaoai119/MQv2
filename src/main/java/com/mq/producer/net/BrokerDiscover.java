package com.mq.producer.net;

import com.alibaba.fastjson.JSON;
import com.mq.producer.meta.SingletonProducerTopicManager;
import com.mq.producer.meta.ProducerTopicManager;
import com.rpc.discovery.ServiceInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/3/23
 */
public class BrokerDiscover {
    //监听列表
    ProducerTopicManager producerTopicManager;
    public BrokerDiscover() {
        producerTopicManager = SingletonProducerTopicManager.getInstance();
    }
}
