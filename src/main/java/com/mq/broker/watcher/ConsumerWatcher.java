package com.mq.broker.watcher;

import com.mq.broker.meta.ConsumerManager;
import com.mq.broker.meta.SingletonConsumerManager;
import com.mq.common.role.BrokerMetaInfo;
import com.mq.producer.meta.SingletonProducerTopicManager;
import com.mq.watcher.UpdateStratage;
import com.mq.watcher.ZooKeeperWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * Created By xfj on 2020/6/3
 * 发现consumer，并在consumerManager中维护consumer信息
 */
public class ConsumerWatcher extends ZooKeeperWatcher {
    ConsumerManager consumerManager;
    BrokerMetaInfo brokerMetaInfo;
    public ConsumerWatcher(String path,BrokerMetaInfo brokerMetaInfo) {
        this.brokerMetaInfo=brokerMetaInfo;
        super.createConnection(path);
        super.setUpdateStratage(new ConsumerWatcherStratage());
    }

    class ConsumerWatcherStratage implements UpdateStratage {

        public ConsumerWatcherStratage() {
            consumerManager = SingletonConsumerManager.getInstance();
        }


        @Override
        public void update() {
            consumerManager.updateConsumeInfos(brokerMetaInfo);
        }
    }
}
