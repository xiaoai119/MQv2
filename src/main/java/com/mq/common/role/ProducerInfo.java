package com.mq.common.role;

/**
 * Created By xfj on 2020/3/16
 */
public class ProducerInfo extends RoleInfo{

    //broker注册信息
    public ProducerInfo() {
        this.role="producer";
    }

    public ProducerInfo(String name) {
        this.name = name;
        this.role="producer";
    }
}
