package com.mq.test;

import com.mq.common.Topic;
import com.mq.common.role.ConsumerMetaInfo;
import com.mq.consumer.allocator.Allocator;
import com.mq.consumer.common.Consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * Created By xfj on 2020/6/3
 */
public class TestConsumer1 {
    public static void main(String[] args) throws UnknownHostException {
            ConsumerMetaInfo consumerMetaInfo = new ConsumerMetaInfo(InetAddress.getLocalHost().getHostAddress(), 8120,"consumer1");
            Consumer consumer = new Consumer(consumerMetaInfo);

            ArrayList<Integer> integers = new ArrayList<>();
            for(int i=0;i<16;i++){
                integers.add(i);
            }
            Topic topic = Allocator.assembleTopic("testTopic1", "broker1", integers);
//            Topic topic1 = Allocator.assembleTopic("testTopic1", "broker2", integers);

            consumer.allocateTopicAndQueueIndex(topic);
//            consumer.allocateTopicAndQueueIndex(topic1);

            integers.clear();
            for(int i=10;i<15;i++){
                integers.add(i);
            }

//            consumer.openDynamicPush();
//            consumer.openSimulateConsumption(3, 200);
            consumer.start();


        }
    }

