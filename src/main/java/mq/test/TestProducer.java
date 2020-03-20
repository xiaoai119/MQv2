package mq.test;

import mq.common.Topic;
import mq.common.message.Message;
import mq.common.message.MessageType;
import mq.producer.DefaultProducer;

import java.util.ArrayList;

/**
 * Created By xfj on 2020/3/16
 */
public class TestProducer {
    public static void main(String[] args) throws InterruptedException {
        DefaultProducer defaultProducer = new DefaultProducer();
        defaultProducer.start();
        Thread.sleep(100);

//        defaultProducer.sendBatch(new Message("test", MessageType.REPLY_EXPECTED, new Topic("test",8),1));
//        defaultProducer.sendBatch(new Message("test1", MessageType.REPLY_EXPECTED,new Topic("test2",5),6));
//
//
//        Thread.sleep(3000);
//        defaultProducer.sendBatch(new Message("test3", MessageType.REPLY_EXPECTED, new Topic("test",3),1));
//        defaultProducer.sendBatch(new Message("test4", MessageType.REPLY_EXPECTED,new Topic("test3",2),6));

        Thread.sleep(2000);
        ArrayList<Message> objects = new ArrayList<>();
        for(int i=0;i<1000;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    defaultProducer.sendBatch(new Message("test", MessageType.REPLY_EXPECTED, new Topic("test",1),6));
                }
            }).start();
        }
    }

}
