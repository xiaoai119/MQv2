package com.mq.test;

        import com.mq.common.message.Message;
        import com.mq.common.message.MessageType;
        import com.mq.producer.common.Producer;

        import java.util.ArrayList;

/**
 * Created By xfj on 2020/6/2
 */
public class TestProducer {
    public static void main(String[] args) throws InterruptedException {
        Producer producer = new Producer();

        ArrayList<Message> objects = new ArrayList<>();

        for(int i=0;i<10;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ArrayList<Message> objects1 = new ArrayList<>();
                    for(int j=0;j<5;j++)
                    objects1.add(new Message("test0", MessageType.REPLY_EXPECTED,"testTopic1", 1));
                    producer.sendBatch(objects1);
                }
            }).start();
        }
    }
}
