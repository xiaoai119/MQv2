package com.mq.producer.net;


import com.mq.producer.meta.SingletonProducerTopicManager;
import com.mq.producer.meta.ProducerTopicManager;
import com.mq.watcher.UpdateStratage;
import com.mq.watcher.ZooKeeperWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created By xfj on 2020/3/23
 */

public class TopicWatcher extends ZooKeeperWatcher{
    private ProducerTopicManager producerTopicManager;
    public TopicWatcher(String path) {
        super.createConnection(path);
        super.setUpdateStratage(new TopicWatcherStratage());
    }

    class TopicWatcherStratage implements UpdateStratage {

        public TopicWatcherStratage() {
            producerTopicManager = SingletonProducerTopicManager.getInstance();
        }


        @Override
        public void update() {
            producerTopicManager.updateTopic();
        }
    }
}


