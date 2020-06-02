package com.mq.test0;

import com.mq.common.message.Message;
import com.mq.producer.Producer;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created By xfj on 2020/3/16
 */
public class TestProducer {
    public static void main(String[] args) throws InterruptedException {
        Producer producer = new Producer();
//        producer.start();
        Thread.sleep(100);

//        producer.sendBatch(new Message("test0", MessageType.REPLY_EXPECTED, new Topic("test0",8),1));
//        producer.sendBatch(new Message("test1", MessageType.REPLY_EXPECTED,new Topic("test2",5),6));
//
//
//        Thread.sleep(3000);
//        producer.sendBatch(new Message("test3", MessageType.REPLY_EXPECTED, new Topic("test0",3),1));
//        producer.sendBatch(new Message("test4", MessageType.REPLY_EXPECTED,new Topic("test3",2),6));

        Random random = new Random();
        Thread.sleep(2000);
        ArrayList<Message> objects = new ArrayList<>();
        for(int i=0;i<500;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
//                    producer.sendBatch(new Message("test0", MessageType.REPLY_EXPECTED, new Topic("test0"+random.nextInt(10) ,1),6));
                }
            }).start();
        }
    }

}
