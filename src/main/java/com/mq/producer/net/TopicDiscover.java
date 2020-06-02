package com.mq.producer.net;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mq.common.QueueInfoOfTopic;
import com.mq.producer.meta.ProducerTopicManager;
import com.mq.producer.meta.SingletonProducerTopicManager;
import com.mq.util.JSONUtil;
import com.mq.common.Topic;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created By xfj on 2020/3/23
 */
public class TopicDiscover {
    ZkClient client;
    private String centerRootPath = "/com/mq";

    public TopicDiscover() {
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
    }

    public Topic getTopic(String path) {
        if (client.exists(path)) {
            List<String> children = client.getChildren(path);
            if(children.isEmpty())return null;
            String s = children.get(0);
            String deCh = null;
            try {
                deCh = URLDecoder.decode(s, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            JSONObject jsonObject = JSON.parseObject(deCh);
            Topic topic = JSONUtil.JSON2Topic(jsonObject);
            System.out.println("获取topic" + topic + JSON.toJSONString(topic));
            return topic;
        }
        return null;
    }

    public List<Topic> getAllTopics() {
        ArrayList<Topic> topics = new ArrayList<Topic>();
        String path = centerRootPath + "/" + "topics";
        if(!client.exists(path)){
            return new ArrayList<>();
        }

        //这层是topicNames
        List<String> children = client.getChildren(path);
        if(!children.isEmpty()){
            for (String s : children) {
                Topic topic = assembleTopic(s);
                topics.add(topic);
                }
            }
        return topics;
    }

    public Topic assembleTopic(String topicName){
        String path = centerRootPath + "/" + "topics"+ "/" + topicName;
        Topic topic = new Topic(topicName);
        HashMap<String,List<Integer>> queues=new HashMap<>();
        List<String> children = client.getChildren(path);
        for (String child : children) {
            String s = child;
            String deCh = null;
            try {
                deCh = URLDecoder.decode(s, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            QueueInfoOfTopic queueInfoOfTopic = JSON.parseObject(deCh, QueueInfoOfTopic.class);
            queues.put(queueInfoOfTopic.getBrokerName(),queueInfoOfTopic.getQueueIndex());
        }
        topic.setQueues(queues);
        return topic;
    }

//    public List<Topic> getAllTopics() {
//        ArrayList<Topic> topics = new ArrayList<Topic>();
//        String path = centerRootPath + "/" + "topics";
//        if(!client.exists(path)){
//            return new ArrayList<>();
//        }
//        List<String> children = client.getChildren(path);
//
//        if(!children.isEmpty()){
//            for (String child : children) {
//                String s = child;
//                String deCh = null;
//                try {
//                    deCh = URLDecoder.decode(s, "UTF-8");
//                } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
//                }
//                JSONObject jsonObject = JSON.parseObject(deCh);
//                Topic topic = JSONUtil.JSON2Topic(jsonObject);
//                topics.add(topic);
//            }
//        }
//
//        return topics;
//    }
}
