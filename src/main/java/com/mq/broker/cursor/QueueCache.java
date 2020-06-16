package com.mq.broker.cursor;

import com.mq.common.message.Message;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created By xfj on 2020/6/12
 * 为每个队列维护一个缓存，发送消息后放入缓存，收到回复后删除
 */
public class QueueCache {
    //外层key为queueIndex，内层key为uuid，value为发送的Message
    ConcurrentHashMap<Integer,ConcurrentHashMap<String,Message>> cache;

    public QueueCache() {
        cache = new ConcurrentHashMap<>();
    }

    public ConcurrentHashMap<String,Message> getMessageByQueueIndex(int queueIndex){
        return cache.get(queueIndex);
    }

    public void add(int queueIndex,Message message){
        String key=message.getUuid();
        message.setTimeStamp(System.currentTimeMillis());
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = cache.getOrDefault(queueIndex,new ConcurrentHashMap<String, Message>());
        stringMessageConcurrentHashMap.put(key,message);
    }

    public void remove(String key,int queueIndex){
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = cache.get(queueIndex);
        stringMessageConcurrentHashMap.remove(key);
    }

    //遍历并返回超时的Message
    public ConcurrentHashMap<Integer,List<Message>> getOutTimeMessage(int outTime){
        ConcurrentHashMap<Integer,List<Message>> result= new ConcurrentHashMap<>();
        for (Map.Entry<Integer, ConcurrentHashMap<String, Message>> entry : cache.entrySet()) {
            ConcurrentHashMap<String, Message> value = entry.getValue();
            List<Message> messages = value.entrySet().stream().map(entry1 -> entry1.getValue()).filter(message -> System.currentTimeMillis() - message.getTimeStamp() > outTime).collect(Collectors.toList());
            result.put(entry.getKey(),messages);
        }
        return result;
    }
}
