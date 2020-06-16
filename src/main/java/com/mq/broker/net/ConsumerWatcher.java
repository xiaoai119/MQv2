package com.mq.broker.net;

import com.mq.broker.meta.ConsumerManager;
import com.mq.broker.meta.SingletonConsumerManager;
import com.mq.common.role.BrokerMetaInfo;
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
        super.createConnection(path);
        consumerManager = SingletonConsumerManager.getInstance();
        this.brokerMetaInfo=brokerMetaInfo;
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
                    consumerManager.updateConsumeInfos(brokerMetaInfo);
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
}
