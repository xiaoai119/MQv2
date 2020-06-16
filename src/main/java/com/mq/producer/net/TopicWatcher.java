package com.mq.producer.net;


import com.mq.producer.meta.SingletonProducerTopicManager;
import com.mq.producer.meta.ProducerTopicManager;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created By xfj on 2020/3/23
 */
public class TopicWatcher extends ZooKeeperWatcher {
    ProducerTopicManager producerTopicManager;

    public TopicWatcher(String path) {
        super.createConnection(path);
        producerTopicManager = SingletonProducerTopicManager.getInstance();
    }

    @Override
    // 递归创建监听
    public void setWatch(String path, ZooKeeper zk) throws Exception {
        if(zk.exists(path, null) == null) {
            return;
        }
        System.out.println("---------setWatch---------" + path);
        List<String> children = zk.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath() + "   >>>>>>>>>" + event.getType().name());
                if(event.getType().name().equals("NodeChildrenChanged")){
//                    通知TopicManager更新event.getPath()下的topic
                    System.out.println("事件path："+event.getPath());
                }
                try {
                    setWatch(path, zk);// 每次监听消费后，需要重新增加Watcher
                    producerTopicManager.updateTopic();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        if(!path.endsWith("Dao")) {
            for(String c : children) {
                String subP = ("/".equals(path)?"":path) + "/" + c;
                setWatch(subP, zk);
            }
        }
    }

//    public static void main(String[] args) {
//        Executors.newSingleThreadExecutor().execute(() -> {
//            TopicWatcher watcher = new TopicWatcher("/com/mq/topics");
//        });
//    }
}
