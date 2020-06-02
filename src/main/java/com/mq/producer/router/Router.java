package com.mq.producer.router;

import com.mq.common.Topic;
import com.mq.common.message.Message;
import com.mq.producer.meta.ProducerTopicManager;
import com.mq.producer.meta.SingletonProducerTopicManager;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created By xfj on 2020/3/23
 */
public class Router {
    ProducerTopicManager producerTopicManager;
    Random random;
    public Router() {
        this.producerTopicManager = SingletonProducerTopicManager.getInstance();
        random = new Random();
    }

    //message的路由
    public RouteTarget routeMessage(Message message){
        //根据message的topic和shardingKey路由
        String topicName = message.getTopic();
        Topic topic = producerTopicManager.getTopic(topicName);
        //获取broker列表
        List<String> brokers = topic.getQueues().keySet().stream().collect(Collectors.toList());
        if(brokers.isEmpty())
        {
            // TODO: 2020/6/1 可抛异常 
        }
        //随机选取broker
        String brokerName = brokers.get(random.nextInt(brokers.size()));
        List<Integer> queues = topic.getQueues().get(brokerName);
        Integer queueIndex=0;

        //根据shardingKey选择具体的queueIndex
        if(message.getShardingKey()!=null){
            queueIndex = queues.get((message.getShardingKey().hashCode() - 1) & queues.size());
        }else{
            queueIndex = queues.get(random.nextInt(queues.size()));
        }
        return new RouteTarget(brokerName,queueIndex);
    }
}
