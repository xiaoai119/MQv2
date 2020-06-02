package com.mq.broker.meta;

import com.mq.broker.common.MyQueue;
import com.mq.common.message.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/3/23
 */
public class QueueManager {
    Integer  queueIndex;
    private ConcurrentHashMap<Integer,ArrayBlockingQueue> queueMap;//队列

    public void sendToBroker(List<Message> messages,Integer key){

    }

    public QueueManager() {
        this.queueIndex = 0;
        queueMap=new ConcurrentHashMap<>();
    }

    public List<Integer> createQueue(int queueNum){
        ArrayList<Integer> index = new ArrayList<>();
        synchronized (queueMap){
            for(int i=0;i<queueNum;i++){
                queueMap.put(queueIndex,new ArrayBlockingQueue(10000));
                index.add(queueIndex);
                queueIndex++;
            }
        }
        return index;
    }
}
