package com.mq.common.role;

import java.util.List;

/**
 * Created By xfj on 2020/6/3
 */
public class ConsumerMetaInfo extends RoleMetaInfo{

    public ConsumerMetaInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.role="consumer";
    }

    public ConsumerMetaInfo(String ip, int port, String name) {
        this.ip = ip;
        this.port = port;
        this.name = name;
        this.role="consumer";
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
