//package com.mq.broker.common;
//
//import com.mq.common.message.Message;
//import com.mq.common.Topic;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// * Created By xfj on 2020/3/16
// */
//public class LoadBalance {
//    private Broker broker;
//
//    public LoadBalance(Broker broker) {
//        this.broker = broker;
//    }
//
//    public List<Integer> balance(Message m){
//        ConcurrentHashMap<Integer,MyQueue> queueMap=broker.getQueueMap();
//        ArrayList<Integer> result = new ArrayList<Integer>();
//        Topic topic = m.getTopic();
//        if(topic==null)
//            topic=new Topic("default",1);
//        int size = queueMap.size();
//        for(int i=0;size+i<topic.getQueueNum();i++){
//            queueMap.put(size+i,new MyQueue());
//        }
//        String shardingKey = m.getShardingKey();
//        if(shardingKey !=null&&topic.getQueueNum()==1) {
//            result.add((shardingKey.hashCode() - 1) & queueMap.size());
//            return result;
//        }
//
//        for(int i=0;i<topic.getQueueNum();i++) {
//            int index = 0;
//            int min = Integer.MAX_VALUE;
//            for(Map.Entry<Integer, MyQueue> entry:queueMap.entrySet()){
//                if(entry.getValue().size()<min&&!result.contains(entry.getKey())) {
//                    min = entry.getValue().size();
//                    index = entry.getKey();
//                }
//            }
//            result.add(index);
//        }
//        return result;
//    }
//}
