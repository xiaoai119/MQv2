package com.mq.broker.meta;

import com.alibaba.fastjson.JSON;
import com.mq.broker.net.ConsumerDiscover;
import com.mq.common.role.BrokerMetaInfo;
import com.mq.common.role.ConsumeInfo;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/6/3
 * broker的consumermanager模块，维护当前的consumer列表，以及对应的queueIndex
 */
public class ConsumerManager {
    //维护一份本地consumer
    ConcurrentHashMap<String,ConsumeInfo> consumeInfos;
    ConsumerDiscover consumerDiscover;
    ConcurrentHashMap<Integer,String> queueIndexOfConsumer;

    public ConsumerManager(){
        consumeInfos=new ConcurrentHashMap<String,ConsumeInfo>();
        consumerDiscover=new ConsumerDiscover();
        queueIndexOfConsumer=new ConcurrentHashMap<Integer,String>();
    }

    public synchronized void updateConsumeInfos(BrokerMetaInfo brokerMetaInfo){
        List<ConsumeInfo> allConsumeInfo = consumerDiscover.getAllConsumeInfo(brokerMetaInfo);
        for (ConsumeInfo consumeInfo : allConsumeInfo) {
            consumeInfos.put(consumeInfo.getConsumerName(),consumeInfo);
            for (Integer i : consumeInfo.getQueues()) {
                queueIndexOfConsumer.put(i,consumeInfo.getConsumerName());
            }
            System.out.println("更新"+consumeInfo.getConsumerName()+":"+ JSON.toJSONString(consumeInfo));
        }
    }

    public ConcurrentHashMap<String, ConsumeInfo> getConsumeInfos() {
        return consumeInfos;
    }

    public ConcurrentHashMap<Integer, String> getQueueIndexOfConsumer() {
        return queueIndexOfConsumer;
    }
}
