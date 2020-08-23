package com.mq.broker.cursor;

import com.mq.broker.meta.ConsumerManager;
import com.mq.broker.meta.MyQueue;
import com.mq.broker.meta.QueueManager;
import com.mq.broker.meta.SingletonConsumerManager;
import com.mq.common.message.Message;
import com.mq.common.role.ConsumeInfo;
import com.mq.consumer.common.AllocateUnit;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/6/12
 * 用于队列的消费记录，维护每个队列的消费进度
 */
public class Cursor {
    static ConsumerManager consumerManager;
    QueueManager queueManager;
    int maxBatchSize;
    Random random;
    static ConcurrentHashMap<String, Integer> cursor;

    public Cursor(QueueManager queueManager,int maxBatchSize) {
        this.queueManager = queueManager;
        this.maxBatchSize=maxBatchSize;
        random=new Random();
        consumerManager= SingletonConsumerManager.getInstance();
        cursor = new ConcurrentHashMap<>();
    }

    private void init(){
        //初始化cursor
        cursor = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ConsumeInfo> consumeInfos = consumerManager.getConsumeInfos();
        for (Map.Entry<String, ConsumeInfo> entry : consumeInfos.entrySet()) {
            cursor.put(entry.getKey(),0);
        }
    }

    public static void updateCursor(){
        ConcurrentHashMap<String, ConsumeInfo> consumeInfos = consumerManager.getConsumeInfos();
        for (Map.Entry<String, ConsumeInfo> entry : consumeInfos.entrySet()) {
            if(cursor.containsKey(entry.getKey()))cursor.put(entry.getKey(),0);
        }
    }

    //传入参数为:String consumerName, String topicName, List<Integer> indexes,int transSize
    //将需要发送的message drainTo到list中
    public HashMap<Integer,List<Message>> getMessagesToSend(List<Integer> indexes,int tranSize,String consumerName){
        //这里采取的策略是随机分配
        int maxIter=100;
        HashMap<Integer, List<Message>> result = new HashMap<>();
        int sendSize=tranSize<maxBatchSize?tranSize:maxBatchSize;
        int iter=0;
        while(sendSize>0&&iter<maxIter){
            iter++;
            Integer index = indexes.get(random.nextInt(indexes.size()));
            List<Message> messages=null;
            synchronized (this){
                Integer integer = cursor.getOrDefault(consumerName+index.toString(),0);
                //从cursor获取消费记录
                messages = queueManager.getMessages(index, sendSize, integer);
                cursor.put(consumerName+index.toString(),cursor.getOrDefault(consumerName+index.toString(),0)+sendSize);
            }

            sendSize-=messages.size();
            List<Message> messages1 = result.getOrDefault(index,new ArrayList<>());
            messages1.addAll(messages);
            result.put(index,messages1);
        }
        return result;
    }
}
