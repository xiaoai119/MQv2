package com.mq.consumer.meta;

import com.mq.producer.meta.SingletonProducerTopicManager;

/**
 * Created By xfj on 2020/6/3
 */
public class SingletonConsumerTopicManger {
    private static ConsumerTopicManager instance;
    private SingletonConsumerTopicManger(){}
    public static ConsumerTopicManager getInstance() {
        if (instance == null) {
            synchronized(SingletonConsumerTopicManger.class){
                if(instance==null)
                    instance = new ConsumerTopicManager();
            }
        }
        return instance;
    }
}
