package com.mq.producer.meta;

import com.mq.common.message.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/3/23
 */
public class SendQueueManager {
    //为每个目标队列维护一个阻塞队列，用于批量发送
    ConcurrentHashMap<String,ArrayBlockingQueue> queueMap;

    public SendQueueManager() {
        this.queueMap = new ConcurrentHashMap<>();
    }

    public ArrayBlockingQueue getQueue(String key){
        if(queueMap.containsKey(key)){
            return queueMap.get(key);
        }else{
            ArrayBlockingQueue arrayBlockingQueue = new ArrayBlockingQueue<Message>(10000);
            queueMap.put(key,arrayBlockingQueue);
            return arrayBlockingQueue;
        }
    }
}
