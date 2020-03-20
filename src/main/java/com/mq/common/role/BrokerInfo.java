package com.mq.common.role;

import java.util.List;

/**
 * Created By xfj on 2020/3/15
 */
public class BrokerInfo extends RoleInfo {
    List<String> topics;
    //broker注册信息

    public BrokerInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.role="broker";
    }

    public BrokerInfo(String ip, int port, String name,List<String> tl) {
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

}
