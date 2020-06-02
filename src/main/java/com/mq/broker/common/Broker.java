package com.mq.broker.common;

import com.mq.broker.meta.QueueManager;
import com.mq.broker.meta.SingletonQueueManager;
import com.mq.broker.meta.BrokerTopicManager;
import com.mq.broker.net.ZookeeperBrokerRegister;
import com.mq.common.message.Message;
import com.mq.common.message.MessageType;
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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/3/15
 */
public class Broker {
    private volatile int count = 0;//记录队列编号
    private int pushTime = 1000;//push时间默认一秒一次
    private int retryTime = 16;//发送失败重试次数
    private ConcurrentHashMap<Integer,MyQueue> queueMap;//队列
    private BrokerMetaInfo brokerMetaInfo;
    private BrokerTopicManager brokerTopicManager;
    private QueueManager queueManager;


    public Broker(BrokerMetaInfo brokerMetaInfo){
        this.brokerMetaInfo = brokerMetaInfo;
        queueMap=new ConcurrentHashMap<Integer, MyQueue>();
        queueManager = SingletonQueueManager.getInstance();
        brokerTopicManager=new BrokerTopicManager(brokerMetaInfo);

    }


    public void init() {
        ZookeeperBrokerRegister register = new ZookeeperBrokerRegister();
        String protocol = PropertiesUtils.getProperties("rpc.protocol");
        BrokerService brokerService = new BrokerServiceImpl(this);
        ServiceObject so = new ServiceObject(BrokerService.class.getName(), BrokerService.class, brokerService);
        ArrayList<ServiceObject> serviceObjects = new ArrayList<>();
        serviceObjects.add(so);
        register.registerBrokerAndService(brokerMetaInfo,serviceObjects,protocol);
        RequestHandler reqHandler = new RequestHandler(new JavaSerializeMessageProtocol(), register);
        RpcServer server = new NettyRpcServer(brokerMetaInfo.getPort(), protocol, reqHandler);
        server.start();
    }

    public void start(){
        init();
    }


    public void rigisterTopics(List<Topic> topics){
        for (Topic topic : topics) {
            updateTopic(topic);
        }
    }

    public boolean rigisterTopic(Topic topic){
        return updateTopic(topic);
    }

    public boolean updateTopic(Topic topic){
        return brokerTopicManager.updateTopic(topic);
    }

    public boolean  addToBroker(Message message){
//        LoadBalance loadBalance = new LoadBalance(this);
//        List<Integer> balance = loadBalance.balance(message);
//        for(Integer i:balance) {
//            this.add(i, message);
//            // TODO: 2020/3/19  测试用
//            System.out.println(brokerMetaInfo.getName()+"添加消息"+message.getUuid()+"到队列"+i);
//        }
        return true;
    }

    public Message addAndReply(Message message){
        if(addToBroker(message)){
            Message ack = new Message("ACK", MessageType.ACK, message.getNum());
            ack.setUuid(message.getUuid());
            return ack;
        }
        return null;
    }

    public synchronized void add(int queueNumber, Message value) {
        MyQueue queue = queueMap.get(queueNumber);
        queue.putAtHeader(value);
    }

    public static void main(String[] args) {
        BrokerMetaInfo brokerMetaInfo = new BrokerMetaInfo("127.0.0.1", 8118,"broker1",new ArrayList<String>());
        Broker broker = new Broker(brokerMetaInfo);
        broker.start();
    }

    public ConcurrentHashMap<Integer, MyQueue> getQueueMap() {
        return queueMap;
    }
}
