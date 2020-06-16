package com.mq.consumer.common;

import com.mq.broker.service.BrokerService;
import com.mq.common.Topic;
import com.mq.common.message.Message;
import com.mq.common.role.ConsumerMetaInfo;
import com.mq.consumer.allocator.Allocator;
import com.mq.consumer.meta.ConsumerMessageProcessor;
import com.mq.consumer.net.ZookeeperConsumerRigister;
import com.mq.consumer.service.ConsumerService;
import com.mq.consumer.service.ConsumerServiceImpl;
import com.mq.producer.net.RPCManager;
import com.rpc.common.protocol.JavaSerializeMessageProtocol;
import com.rpc.server.NettyRpcServer;
import com.rpc.server.RequestHandler;
import com.rpc.server.RpcServer;
import com.rpc.server.register.ServiceObject;
import com.rpc.util.PropertiesUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/6/3
 */
public class Consumer {
    private ConsumerMetaInfo consumerMetaInfo;
    private RPCManager rpcManager;
    private ConsumerMessageProcessor consumerMessageProcessor;
    private Allocator allocator;

    public Consumer(ConsumerMetaInfo consumerMetaInfo) {
        rpcManager = new RPCManager();
        allocator = new Allocator(consumerMetaInfo);
        this.consumerMetaInfo = consumerMetaInfo;
        this.consumerMessageProcessor = new ConsumerMessageProcessor(this.consumerMetaInfo, rpcManager, allocator, 100, 0.8);
    }

    public void init() {
        resisterAndListen();
    }

    private void resisterAndListen() {
        ZookeeperConsumerRigister register = new ZookeeperConsumerRigister();
        String protocol = PropertiesUtils.getProperties("rpc.protocol");
        ConsumerService consumerService = new ConsumerServiceImpl(this);
        ServiceObject so = new ServiceObject(ConsumerService.class.getName(), ConsumerService.class, consumerService);
        ArrayList<ServiceObject> serviceObjects = new ArrayList<>();
        serviceObjects.add(so);
        register.registerConsumerAndService(consumerMetaInfo, serviceObjects, protocol);
        RequestHandler reqHandler = new RequestHandler(new JavaSerializeMessageProtocol(), register);
        RpcServer server = new NettyRpcServer(consumerMetaInfo.getPort(), protocol, reqHandler);
        server.start();
    }

    public void allocateTopicAndQueueIndex(Topic topic) {
        allocator.allocateTopicAndQueueIndex(topic);
    }

    public void openSimulateConsumption(int size,int timeUnit){
        consumerMessageProcessor.simulateConsumption(size,timeUnit);
    }

    public void allocateTopicsAndQueueIndex(List<Topic> topic) {
        allocator.allocateTopicsAndQueueIndex(topic);
    }

    public void openDynamicPush(){
        consumerMessageProcessor.timedSendPushRequest();
    }

    public static Topic assembleTopic(String topicName ,String brokerName,List<Integer> index){
        return Allocator.assembleTopic(topicName,brokerName,index);
    }

    public static Topic assembleTopic(Topic topic ,String brokerName,List<Integer> index){
        return Allocator.assembleTopic(topic,brokerName,index);
    }

    public void relese(int size){
        consumerMessageProcessor.relese(size);
    }

    public void addToBuffer(List<Message> messages){
        consumerMessageProcessor.addToBuffer(messages);
    }

    public void start() {
        init();
    }
}
