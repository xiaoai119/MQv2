package com.mq.broker.net;

import com.alibaba.fastjson.JSON;
import com.mq.common.QueueInfoOfTopic;
import com.mq.common.Topic;
import com.mq.common.role.BrokerMetaInfo;
import com.mq.common.role.ConsumeInfo;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created By xfj on 2020/6/10
 */
public class ConsumerDiscover {
    ZkClient client;
    private String centerRootPath = "/com/mq";

    public ConsumerDiscover() {
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
    }

    public List<ConsumeInfo> getAllConsumeInfo(BrokerMetaInfo brokerMetaInfo) {
        ArrayList<ConsumeInfo> consumeInfos = new ArrayList<ConsumeInfo>();
        String path = centerRootPath + "/" + "consumer";
        if(!client.exists(path)){
            return new ArrayList<>();
        }

        //这层是topicNames
        List<String> children = client.getChildren(path);
        if(!children.isEmpty()){
            for (String s : children) {
                ConsumeInfo consumeInfo = assembleConsumeInfo(s,brokerMetaInfo);
                consumeInfos.add(consumeInfo);
            }
        }
        return consumeInfos;
    }

    private ConsumeInfo assembleConsumeInfo(String consumerName,BrokerMetaInfo brokerMetaInfo) {
        String path = centerRootPath + "/" + "consumer"+ "/" + consumerName;
        ConsumeInfo consumeInfo = new ConsumeInfo(consumerName);
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
            if(queueInfoOfTopic.getBrokerName().equals(brokerMetaInfo.getName()))
                consumeInfo.getQueues().addAll(queueInfoOfTopic.getQueueIndex());
        }

        return consumeInfo;

    }

//    public Topic assembleTopic(String topicName){
//        String path = centerRootPath + "/" + "topics"+ "/" + topicName;
//        Topic topic = new Topic(topicName);
//        HashMap<String,List<Integer>> queues=new HashMap<>();
//        List<String> children = client.getChildren(path);
//        for (String child : children) {
//            String s = child;
//            String deCh = null;
//            try {
//                deCh = URLDecoder.decode(s, "UTF-8");
//            } catch (UnsupportedEncodingException e) {
//                e.printStackTrace();
//            }
//            QueueInfoOfTopic queueInfoOfTopic = JSON.parseObject(deCh, QueueInfoOfTopic.class);
//            queues.put(queueInfoOfTopic.getBrokerName(),queueInfoOfTopic.getQueues());
//        }
//        topic.setQueues(queues);
//        return topic;
//    }

    public static void main(String[] args) {
        BrokerMetaInfo brokerMetaInfo = new BrokerMetaInfo("127.0.0.1", 8118,"broker1",new ArrayList<Topic>());

        ConsumerDiscover consumerDiscover = new ConsumerDiscover();
        consumerDiscover.getAllConsumeInfo(brokerMetaInfo);
    }
}
