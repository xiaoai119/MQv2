package com.mq.consumer.net;

import com.alibaba.fastjson.JSON;
import com.mq.common.QueueInfoOfTopic;
import com.mq.common.role.ConsumerMetaInfo;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

/**
 * Created By xfj on 2020/6/3
 */
public class ConsumerTopicRegister {
    ZkClient client;
    private String centerRootPath = "/com/mq";

    public ConsumerTopicRegister() {
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
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

    public void updateTopic(QueueInfoOfTopic queueInfoOfTopic, ConsumerMetaInfo consumerMetaInfo) {
        String topicUri = getQueueInfoUri(queueInfoOfTopic);
        String servicePath = centerRootPath + "/" + "consumer"+"/" + consumerMetaInfo.getName();
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

    public void deleteAll(ConsumerMetaInfo consumerMetaInfo){
        String servicePath = centerRootPath + "/" + "consumer"+"/" + consumerMetaInfo.getName();
        List<String> children = client.getChildren(servicePath);
        for (String child : children) {
            client.delete(servicePath+"/"+child);
        }
    }
}
