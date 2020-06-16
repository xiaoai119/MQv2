package com.mq.common.role;

import com.mq.common.Topic;

import java.util.List;

/**
 * Created By xfj on 2020/3/15
 */
public class BrokerMetaInfo extends RoleMetaInfo {
    List<Topic> topics;
    //broker注册信息

    public BrokerMetaInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.role="broker";
    }



    public BrokerMetaInfo(String ip, int port, String name, List<Topic> tl) {
        this.ip = ip;
        this.port = port;
        this.topics = tl;
        this.name = name;
        this.role="broker";
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;
    }
}
