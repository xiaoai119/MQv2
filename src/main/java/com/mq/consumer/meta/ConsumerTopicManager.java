package com.mq.consumer.meta;

import com.alibaba.fastjson.JSON;
import com.mq.common.QueueInfoOfTopic;
import com.mq.common.Topic;
import com.mq.common.role.ConsumerMetaInfo;
import com.mq.consumer.net.ConsumerTopicRegister;
import com.mq.consumer.net.TopicDiscover;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/6/3
 * 发现broker的topics，并维护一份本地列表
 */
public class ConsumerTopicManager {
    //维护一份本地topic
    ConcurrentHashMap<String,Topic> topics;
    TopicDiscover topicDiscover;
    ConsumerTopicRegister consumerTopicRegister;


    public ConsumerTopicManager(){
        this.topics = new ConcurrentHashMap<>();
        topicDiscover=new TopicDiscover();
        initTopics();
        consumerTopicRegister=new ConsumerTopicRegister();
    }

    public void initTopics(){
        List<Topic> allTopics = topicDiscover.getAllTopics();
        for (Topic topic : allTopics) {
            System.out.println("获取topic"+topic.getTopicName()+ JSON.toJSONString(topic));
            topics.put(topic.getTopicName(),topic);
        }
    }

    public void addTopic(Topic topic){
        topics.put(topic.getTopicName(),topic);
    }

    public ConcurrentHashMap<String, Topic> getTopics() {
        return topics;
    }

    public void setTopics(ConcurrentHashMap<String, Topic> topics) {
        this.topics = topics;
    }

    /**
     * 更新本地topic列表
     */
    public void updateTopic(){
        List<Topic> allTopics = topicDiscover.getAllTopics();
        for (Topic topic : allTopics) {
            topics.put(topic.getTopicName(),topic);
            System.out.println("producer更新topic"+topic.getTopicName()+ JSON.toJSONString(topic));
        }
    }

    public Topic getTopic(String topicNmae){
        if(!topics.containsKey(topicNmae)){
            return null;
        }
        return topics.get(topicNmae);
    }


    /**
     * 更新当前consumer消费的topic，可被borker发现
     * @param consumerMetaInfo
     * @param topic
     */
    public void updateTopicOfConsumer(ConsumerMetaInfo consumerMetaInfo,Topic topic){
        HashMap<String, List<Integer>> queues = topic.getQueues();
        for (Map.Entry<String, List<Integer>> entry : queues.entrySet()) {
            consumerTopicRegister.updateTopic(new QueueInfoOfTopic(topic.getTopicName(),entry.getKey(),entry.getValue()),consumerMetaInfo);
        }
    }

    public void deleteAllTopics(ConsumerMetaInfo consumerMetaInfo){
        consumerTopicRegister.deleteAll(consumerMetaInfo);
    }
}
