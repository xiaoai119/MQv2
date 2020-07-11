package com.mq.broker.meta;

import com.alibaba.fastjson.JSON;
import com.mq.broker.rigister.ZookeeperTopicRegister;
import com.mq.common.QueueInfoOfTopic;
import com.mq.common.Topic;
import com.mq.common.role.BrokerMetaInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/3/23
 * 负责topic的创建，更新等
 * 并向zk注册和更新topicMeta
 */
public class BrokerTopicManager {
    BrokerMetaInfo brokerMetaInfo;
    ConcurrentHashMap<Topic,List<Integer>> topics;//当前节点持有的topic,以及队列索引
    QueueManager queueManager;
    ZookeeperTopicRegister zookeeperTopicRegister;

    public BrokerTopicManager(BrokerMetaInfo brokerMetaInfo) {
        topics=new ConcurrentHashMap<>();
        this.brokerMetaInfo = brokerMetaInfo;
        this.queueManager = SingletonQueueManager.getInstance();
        zookeeperTopicRegister=new ZookeeperTopicRegister();
    }

    public boolean updateTopic(Topic topic){
        List<Integer> queues =new ArrayList<>();
        if(!topics.containsKey(topic)){
            //queueManager申请队列
            queues = queueManager.createQueue(topic.getQueueNum());
            topics.put(topic,queues);
            HashMap<String, List<Integer>> queuesOfTpoic = topic.getQueues();
            queuesOfTpoic.put(brokerMetaInfo.getName(),queues);
        }else{
            //变更队列
            queues = topics.get(topic);
            if(queues.size()!=topic.getQueueNum()){
                if(queues.size()==topic.getQueueNum())
                    return true;
                if(queues.size()<topic.getQueueNum()){
                    // TODO: 2020/3/23 删除队列
                }
                if(queues.size()>topic.getQueueNum()){
                    //继续申请队列
                    List<Integer> queue = queueManager.createQueue(topic.getQueueNum() - queues.size());
                    queues.addAll(queue);
                    HashMap<String, List<Integer>> queuesOfTpoic = topic.getQueues();
                    queuesOfTpoic.put(brokerMetaInfo.getName(),queue);
                }
            }
        }
        //向zk注册或更新topic信息
        zookeeperTopicRegister.updateTopic(new QueueInfoOfTopic(topic.getTopicName(),brokerMetaInfo.getName(),queues));
        System.out.println(brokerMetaInfo.getName()+"注册Topic"+topic.getTopicName()+ JSON.toJSONString(topic));
        return true;
    }

    public static void main(String[] args) throws UnknownHostException {
        BrokerMetaInfo brokerMetaInfo = new BrokerMetaInfo(InetAddress.getLocalHost().getHostAddress(), 8118,"broker3",new ArrayList<Topic>());
        BrokerTopicManager brokerTopicManager = new BrokerTopicManager(brokerMetaInfo);
        Topic testTopic = new Topic("testTopic3",10);
        brokerTopicManager.updateTopic(testTopic);
        ZookeeperTopicRegister zookeeperTopicRegister = new ZookeeperTopicRegister();
//        Topic topic = zookeeperTopicRegister.getTopic("testTopic");
        System.out.println();
    }
}
