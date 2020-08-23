package com.mq.consumer.net;

import com.mq.consumer.meta.ConsumerTopicManager;
import com.mq.consumer.meta.SingletonConsumerTopicManger;
import com.mq.producer.meta.SingletonProducerTopicManager;
import com.mq.watcher.UpdateStratage;
import com.mq.watcher.ZooKeeperWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created By xfj on 2020/6/3
 */
public class TopicWatcher extends ZooKeeperWatcher {
    ConsumerTopicManager consumerTopicManager;
    int transid=0;

    public TopicWatcher(String path) {
        super.createConnection(path);
        consumerTopicManager = SingletonConsumerTopicManger.getInstance();
        super.setUpdateStratage(new TopicWatcherStratage());
    }

    class TopicWatcherStratage implements UpdateStratage {

        public TopicWatcherStratage() {
            consumerTopicManager = SingletonConsumerTopicManger.getInstance();
        }


        @Override
        public void update() {
            consumerTopicManager.updateTopic(transid++);
        }
    }
}
