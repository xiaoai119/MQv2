package com.mq.broker.meta;

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
    private ConcurrentHashMap<Integer,MyQueue> queueMap;//队列

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
                queueMap.put(queueIndex,new MyQueue());
                index.add(queueIndex);
                queueIndex++;
            }
        }
        return index;
    }

    public MyQueue getQueue(int queueIndex){
//        return queueMap.getOrDefault(queueIndex,new MyQueue());
        return queueMap.get(queueIndex);
    }

    public synchronized List<Message> getMessages(int queueIndex,int maxSize){
        MyQueue queue = getQueue(queueIndex);
        List<Message> messages = queue.drainTo(maxSize);
        return messages;
    }

}
