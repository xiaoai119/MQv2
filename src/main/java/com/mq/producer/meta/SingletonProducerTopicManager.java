package com.mq.producer.meta;

/**
 * Created By xfj on 2020/3/23
 */
public class SingletonProducerTopicManager {
    private static ProducerTopicManager instance;
    private SingletonProducerTopicManager(){}
    public static ProducerTopicManager getInstance() {
        if (instance == null) {
            synchronized(SingletonProducerTopicManager.class){
                if(instance==null)
                    instance = new ProducerTopicManager();
            }
        }
        return instance;
    }
}
