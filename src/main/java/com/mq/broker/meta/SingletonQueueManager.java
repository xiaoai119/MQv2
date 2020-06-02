package com.mq.broker.meta;

import com.mq.rigister.SigletonZookeeperRoleAndServiceRegister;
import com.mq.rigister.ZookeeperRoleAndServiceInfoRegister;

/**
 * Created By xfj on 2020/3/23
 */
public class SingletonQueueManager {
    private static QueueManager instance;
    private SingletonQueueManager(){}
    public static QueueManager getInstance() {
        if (instance == null) {
            synchronized(SingletonQueueManager.class){
                if(instance==null)
                    instance = new QueueManager();
            }
        }
        return instance;
    }
}
