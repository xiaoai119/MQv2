package com.mq.consumer.allocator;

import com.alibaba.fastjson.JSON;
import com.mq.common.Topic;
import com.mq.common.role.ConsumerMetaInfo;
import com.mq.consumer.common.AllocateUnit;
import com.mq.consumer.meta.ConsumerTopicManager;
import com.mq.consumer.meta.SingletonConsumerTopicManger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created By xfj on 2020/6/3
 * 分区分配模块，分配consumer从哪个分区进行消费，关联consumer与broker的queueIndex
 */
public class Allocator {
    ConsumerTopicManager consumerTopicManager;
    ConsumerMetaInfo consumerMetaInfo;
    //维护一份本地分配的topic以及队列<topicName,topic>
    ConcurrentHashMap<String,Topic> topicsOfConsumer;
    List<AllocateUnit> allocateUnits;
    Random random;

    public Allocator(ConsumerMetaInfo consumerMetaInfo) {
        this.consumerTopicManager = SingletonConsumerTopicManger.getInstance();
        this.consumerMetaInfo=consumerMetaInfo;
        allocateUnits=new ArrayList<>();
        topicsOfConsumer=new ConcurrentHashMap<String,Topic>();
        random=new Random();
    }

    public void allocateTopicsAndQueueIndex(List<Topic> topics){
        List<Topic> topics1 = filterTopicsAndQueueIndex(topics);
        for (Topic topic : topics1) {
            consumerTopicManager.updateTopicOfConsumer(consumerMetaInfo,topic);
            List<Map.Entry<String, List<Integer>>> entryList = topic.getQueues().entrySet().stream().collect(Collectors.toList());
            //向本地添加可分配的单元
            allocateUnits.add(new AllocateUnit(topic.getTopicName(),entryList.get(0).getKey(),entryList.get(0).getValue()));
            topicsOfConsumer.put(topic.getTopicName(),topic);
            System.out.println("向"+consumerMetaInfo.getName()+"分配Topic"+topic.getTopicName()+ JSON.toJSONString(topic));
        }
    }

    public void allocateTopicAndQueueIndex(Topic topic){
        ArrayList<Topic> topics = new ArrayList<>();
        topics.add(topic);
        allocateTopicsAndQueueIndex(topics);
    }

    public void allocateTopics(List<Topic> topics){
        List<Topic> topics1 = filterTopics(topics);
        for (Topic topic : topics1) {
            consumerTopicManager.updateTopicOfConsumer(consumerMetaInfo,topic);
            List<Map.Entry<String, List<Integer>>> entryList = topic.getQueues().entrySet().stream().collect(Collectors.toList());
            //向本地添加可分配的单元
            allocateUnits.add(new AllocateUnit(topic.getTopicName(),entryList.get(0).getKey(),entryList.get(0).getValue()));
            topicsOfConsumer.put(topic.getTopicName(),topic);
            System.out.println("向"+consumerMetaInfo.getName()+"分配Topic"+topic.getTopicName()+ JSON.toJSONString(topic));
        }
    }

    public static Topic assembleTopic(String topicName ,String brokerName,List<Integer> index){
        HashMap<String, List<Integer>> stringListHashMap = new HashMap<>();
        stringListHashMap.put(brokerName,index);
        return new Topic(topicName,stringListHashMap);
    }

    public static Topic assembleTopic(Topic topic ,String brokerName,List<Integer> index){
        HashMap<String, List<Integer>> stringListHashMap = new HashMap<>();
        stringListHashMap.put(brokerName,index);
        topic.getQueues().put(brokerName,index);
        return topic;
    }

    public void randomAllocate(Topic topic){
        ArrayList<Topic> topics = new ArrayList<>();
        topics.add(topic);
        allocateTopics(topics);
    }

    private List<Topic> filterTopics(List<Topic> topics) {
        ArrayList<Topic> result = new ArrayList<>();
        ConcurrentHashMap<String, Topic> currentTopics = consumerTopicManager.getTopics();
        for (Topic topic : topics) {
            if(currentTopics.containsKey(topic.getTopicName())){
                Topic topic1 = currentTopics.get(topic.getTopicName());
                result.add(topic1);
            }
        }
        return result;
    }

    private List<Topic> filterTopicsAndQueueIndex(List<Topic> topics) {
        ArrayList<Topic> result = new ArrayList<>();
        ConcurrentHashMap<String, Topic> currentTopics = consumerTopicManager.getTopics();
        for (Topic topic : topics) {
            if(currentTopics.containsKey(topic.getTopicName())){
                Topic topicFlitered = new Topic(topic.getTopicName());

                Topic topic1 = currentTopics.get(topic.getTopicName());
                HashMap<String, List<Integer>> queues1 = topic1.getQueues();

                HashMap<String, List<Integer>> queues = topic.getQueues();

                for (Map.Entry<String, List<Integer>> entry : queues.entrySet()) {
                    if(queues1.containsKey(entry.getKey())){
                        ArrayList<Integer> integers1 = new ArrayList<>();


                        List<Integer> integers = queues1.get(entry.getKey());

                        //value为申请的queueId
                        List<Integer> value = entry.getValue();
                        for (Integer integer : value) {
                            if(integers.contains(integer))
                                integers1.add(integer);
                        }
                        topicFlitered.getQueues().put(entry.getKey(),integers1);
                    }
                }
                result.add(topicFlitered);
            }
        }
        return result;
    }

    //当broker刷新时，需要刷新conumer的信息
    public void refreshTopics(){
        consumerTopicManager.deleteAllTopics(consumerMetaInfo);
        List<Topic> topics = topicsOfConsumer.entrySet().stream().map(entry -> entry.getValue()).collect(Collectors.toList());
        allocateTopicsAndQueueIndex(topics);
    }

    public ConcurrentHashMap<String, Topic> getTopicsOfConsumer() {
        return topicsOfConsumer;
    }

    /**
     * 随机选择一个broker中的一个topic
     */
    public Topic randomAllocate(){
        //todo 根据broker负载进行路由，这里先随机选择
        AllocateUnit allocateUnit = allocateUnits.get(random.nextInt(allocateUnits.size()));
        Topic result = new Topic(allocateUnit.getTopicName());
        HashMap<String, List<Integer>> queues = result.getQueues();
        queues.put(allocateUnit.getBrokerName(),allocateUnit.getQueues());
        return result;
    }
}
