package com.mq.producer.meta;

import com.alibaba.fastjson.JSON;
import com.mq.common.Topic;
import com.mq.producer.net.TopicDiscover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/3/23
 */
public class ProducerTopicManager {
    //维护一份本地发送历史topic
    ConcurrentHashMap<String,Topic> topics;
    TopicDiscover topicDiscover;

    public ProducerTopicManager(){
        this.topics = new ConcurrentHashMap<>();
        topicDiscover=new TopicDiscover();
        initTopics();
    }

    public void initTopics(){
        List<Topic> allTopics = topicDiscover.getAllTopics();
        for (Topic topic : allTopics) {
            System.out.println("获取topic"+topic.getTopicName());
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
}
