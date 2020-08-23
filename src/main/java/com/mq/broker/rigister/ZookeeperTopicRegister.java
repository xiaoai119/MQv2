package com.mq.broker.rigister;

import com.alibaba.fastjson.JSON;
import com.mq.common.QueueInfoOfTopic;
import com.mq.common.Topic;
import com.mq.zklock.ZooKeeperDistributionLock;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * Created By xfj on 2020/3/23
 */
public class ZookeeperTopicRegister {
    ZkClient client;
    private String centerRootPath = "/com/mq";
    ZooKeeperDistributionLock zooKeeperDistributionLock;
    public ZookeeperTopicRegister() {
        zooKeeperDistributionLock = new ZooKeeperDistributionLock(centerRootPath + "/lock");
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
    }

    public boolean updateTopic(QueueInfoOfTopic queueInfoOfTopic) {
        if(!client.exists(centerRootPath + "/" + "topics")){
            client.createPersistent(centerRootPath + "/" + "topics");
        }
        String path= centerRootPath + "/" + "topics" + "/" +  queueInfoOfTopic.getTopicName();
        if(!client.exists(path)){
            client.createPersistent(path);
        }

        zooKeeperDistributionLock.lock();
        //检查该节点下是否已存在对应的broker节点，若有则先删除再创建
        List<String> children = client.getChildren(path);
        for (String child : children) {
            String deCh = null;
        try {
            deCh = URLDecoder.decode(child, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
            QueueInfoOfTopic queueInfoOfTopic1 = JSON.parseObject(deCh, QueueInfoOfTopic.class);
            if(queueInfoOfTopic.getBrokerName().equals(queueInfoOfTopic1.getBrokerName()))
                if(!client.delete(path+"/"+child))
                    return false;
        }

        createTopicNode(queueInfoOfTopic);
        zooKeeperDistributionLock.unlock();
        return true;

//        if(!client.exists(path)){
//            createTopicNode(queueInfoOfTopic);
//            return true;
//        }
//        List<String> children = client.getChildren(path);
//        //若无子节点
//        if(children.size()==0){
//            createTopicNode(queueInfoOfTopic);
//            return true;
//        }

        //更新topic在当前broker的队列信息
        //这里应该是分布式锁，抢到锁更新topic，这里先返回false
//        if(children.size()>1)
//            return false;
//        String s = children.get(0);
//        String deCh = null;
//        try {
//            deCh = URLDecoder.decode(s, "UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//        Topic old = JSON.parseObject(deCh, Topic.class);
//        for (Map.Entry<String, List<Integer>> entry : topic.getQueues().entrySet()) {
//            old.getQueues().put(entry.getKey(),entry.getValue());
//        }
//        System.out.println("更新节点"+path+"/"+children.get(0));
//        createTopicNode(old);
//        client.delete(path+"/"+children.get(0));
//        return true;
    }

    private void createTopicNode(QueueInfoOfTopic queueInfoOfTopic) {
        String topicUri = getQueueInfoUri(queueInfoOfTopic);
        String servicePath = centerRootPath + "/" + "topics"+"/" + queueInfoOfTopic.getTopicName();
        if (!client.exists(servicePath)) {
            System.out.println("创建节点"+servicePath);
            client.createPersistent(servicePath, true);
        }
        String uriPath = servicePath + "/" + topicUri;
        if (client.exists(uriPath)) {
            client.delete(uriPath);
        }
        System.out.println("注册Topic节点"+uriPath);
        client.createEphemeral(uriPath);
    }

    private String getTopicUri(Topic topic) {
        String uri = JSON.toJSONString(topic);
        try {
            uri = URLEncoder.encode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return uri;
    }

    private String getQueueInfoUri(QueueInfoOfTopic queueInfoOfTopic) {
        String uri = JSON.toJSONString(queueInfoOfTopic);
        try {
            uri = URLEncoder.encode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return uri;
    }

//    public Topic getTopic(String name){
//        String servicePath = centerRootPath + "/" + "topics" + "/" + name;
//        if (client.exists(servicePath)) {
//            List<String> children = client.getChildren(servicePath);
//            String s = children.get(0);
//            String deCh = null;
//            try {
//                deCh = URLDecoder.decode(s, "UTF-8");
//            } catch (UnsupportedEncodingException e) {
//                e.printStackTrace();
//            }
//            JSONObject jsonObject = JSON.parseObject(deCh);
//            Topic topic = JSONUtil.JSON2Topic(jsonObject);
//            System.out.println("获取topic"+topic+JSON.toJSONString(topic));
//            return topic;
//        }
//        return null;
//    }
}
