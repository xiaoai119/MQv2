package com.mq.broker.common;

import com.mq.broker.cursor.Cursor;
import com.mq.broker.cursor.QueueCache;
import com.mq.broker.meta.*;
import com.mq.broker.watcher.ConsumerWatcher;
import com.mq.broker.net.RPCManager;
import com.mq.broker.rigister.ZookeeperBrokerRegister;
import com.mq.common.message.Message;
import com.mq.broker.service.BrokerService;
import com.rpc.common.protocol.JavaSerializeMessageProtocol;
import com.rpc.server.NettyRpcServer;
import com.rpc.server.RequestHandler;
import com.rpc.server.RpcServer;
import com.rpc.server.register.ServiceObject;
import com.rpc.util.PropertiesUtils;
import com.mq.common.role.BrokerMetaInfo;
import com.mq.common.Topic;
import com.mq.broker.service.BrokerServiceImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Executors;

/**
 * Created By xfj on 2020/3/15
 */
public class Broker {
    private volatile int count = 0;//记录队列编号
    private int pushTime = 1000;//push时间默认一秒一次
    private int retryTime = 16;//发送失败重试次数
    private BrokerMetaInfo brokerMetaInfo;
    private BrokerTopicManager brokerTopicManager;
    private BrokerMessageProcessor brokerMessageProcessor;
    private QueueManager queueManager;
    private ConsumerManager consumerManager;

    private QueueCache queueCache;
    private Cursor cursor;
    private int maxBatch;
    private RPCManager rpcManager;

    public Broker(BrokerMetaInfo brokerMetaInfo) {
        this.brokerMetaInfo = brokerMetaInfo;
        brokerTopicManager = new BrokerTopicManager(brokerMetaInfo);
        //注册初始topics
        updateTopics(brokerMetaInfo.getTopics());

        queueManager = SingletonQueueManager.getInstance();
        consumerManager = SingletonConsumerManager.getInstance();

        rpcManager = new RPCManager();
        queueCache = new QueueCache();
        queueManager = SingletonQueueManager.getInstance();
        maxBatch = 3;
        cursor = new Cursor(queueManager, maxBatch);

        brokerMessageProcessor = new BrokerMessageProcessor(queueCache, queueManager, cursor, maxBatch, rpcManager);

        openConsumeInfoWatcher();
        openReSendToConsumer(1000,500);
    }


    public void init() {
        rigisterAndListen();
    }

    /**
     * 向zk注册brokerservice信息，并开启监听
     */
    private void rigisterAndListen() {
        ZookeeperBrokerRegister register = new ZookeeperBrokerRegister();
        String protocol = PropertiesUtils.getProperties("rpc.protocol");
        BrokerService brokerService = new BrokerServiceImpl(this);
        ServiceObject so = new ServiceObject(BrokerService.class.getName(), BrokerService.class, brokerService);
        ArrayList<ServiceObject> serviceObjects = new ArrayList<>();
        serviceObjects.add(so);
        register.registerBrokerAndService(brokerMetaInfo, serviceObjects, protocol);
        RequestHandler reqHandler = new RequestHandler(new JavaSerializeMessageProtocol(), register);
        RpcServer server = new NettyRpcServer(brokerMetaInfo.getPort(), protocol, reqHandler);
        //开启netty监听
        server.start();
    }

    public void start() {
        init();
    }

    public void rigisterTopics(List<Topic> topics) {
        for (Topic topic : topics) {
            updateTopic(topic);
        }
    }

    public void pushMessage(String consumerName, List<Integer> indexes, int transSize){
        brokerMessageProcessor.pushMessage(consumerName,indexes,transSize);
    }

    private void openConsumeInfoWatcher() {
        Executors.newSingleThreadExecutor().execute(() -> {
            ConsumerWatcher watcher = new ConsumerWatcher("/com/mq/consumer", brokerMetaInfo);
        });
    }

    private void openReSendToConsumer(int outTime,int excuteTime){
//        brokerMessageProcessor.reSendMessage(outTime,excuteTime);
    }

    public boolean rigisterTopic(Topic topic) {
        return updateTopic(topic);
    }

    public boolean updateTopic(Topic topic) {
        return brokerTopicManager.updateTopic(topic);
    }

    public void updateTopics(List<Topic> topics) {
        for (Topic topic : topics) {
            brokerTopicManager.updateTopic(topic);
        }
    }

    public void addToQueue(Message message, Integer queueIndex) {
        brokerMessageProcessor.addToQueue(message, queueIndex);
    }

    public Message addAndReply(Message message, Integer queueIndex) {
        return brokerMessageProcessor.addAndReply(message, queueIndex);
    }
}
