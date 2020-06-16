package com.mq.consumer.net;

import com.mq.consumer.meta.ConsumerTopicManager;
import com.mq.consumer.meta.SingletonConsumerTopicManger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created By xfj on 2020/6/3
 */
public class TopicWatcher extends ZooKeeperWatcher{
    ConsumerTopicManager consumerTopicManager;

    public TopicWatcher(String path) {
        super.createConnection(path);
        consumerTopicManager = SingletonConsumerTopicManger.getInstance();
        while(true) {
            //监听
        }
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
                    consumerTopicManager.updateTopic();
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

    public static void main(String[] args) {
        Executors.newSingleThreadExecutor().execute(() -> {
            TopicWatcher watcher = new TopicWatcher("/com/mq/topics");
        });
    }
}
