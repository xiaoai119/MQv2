package com.mq.broker.cursor;

import com.mq.broker.meta.MyQueue;
import com.mq.broker.meta.QueueManager;
import com.mq.common.message.Message;
import com.mq.consumer.common.AllocateUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created By xfj on 2020/6/12
 * 用于队列的消费记录，维护每个队列的消费进度
 */
public class Cursor {
    QueueManager queueManager;
    int maxBatchSize;
    Random random;

    public Cursor(QueueManager queueManager,int maxBatchSize) {
        this.queueManager = queueManager;
        this.maxBatchSize=maxBatchSize;
        random=new Random();
    }

    //传入参数为:String consumerName, String topicName, List<Integer> indexes,int transSize
    //将需要发送的message drainTo到list中
    public HashMap<Integer,List<Message>> getMessagesToSend(List<Integer> indexes,int tranSize){
        //这里采取的策略是随机分配
        int maxIter=1000;
        HashMap<Integer, List<Message>> result = new HashMap<>();
        int sendSize=tranSize<maxBatchSize?tranSize:maxBatchSize;
        int iter=0;
        while(sendSize>0&&iter<maxIter){
            iter++;
            Integer index = indexes.get(random.nextInt(indexes.size()));
            List<Message> messages = queueManager.getMessages(index, sendSize);
            sendSize-=messages.size();
            List<Message> messages1 = result.getOrDefault(index,new ArrayList<>());
            messages1.addAll(messages);
            result.put(index,messages1);
        }
        return result;
    }
}
