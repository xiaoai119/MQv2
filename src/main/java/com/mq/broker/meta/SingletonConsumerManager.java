package com.mq.broker.meta;

import com.mq.common.role.BrokerMetaInfo;

/**
 * Created By xfj on 2020/6/10
 */
public class SingletonConsumerManager {
    static ConsumerManager instance;
    private SingletonConsumerManager(){}
    public static ConsumerManager getInstance() {
        if (instance == null) {
            synchronized(SingletonConsumerManager.class){
                if(instance==null)
                    instance = new ConsumerManager();
            }
        }
        return instance;
    }
}
