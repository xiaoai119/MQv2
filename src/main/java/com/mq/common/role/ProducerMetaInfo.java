package com.mq.common.role;

/**
 * Created By xfj on 2020/3/16
 */
public class ProducerMetaInfo extends RoleMetaInfo {

    //broker注册信息
    public ProducerMetaInfo() {
        this.role="producer";
    }

    public ProducerMetaInfo(String name) {
        this.name = name;
        this.role="producer";
    }
}
