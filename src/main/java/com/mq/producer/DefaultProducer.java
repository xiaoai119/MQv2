package com.mq.producer;

import com.mq.common.Topic;
import com.mq.common.message.Message;
import com.mq.common.message.MessageType;
import com.mq.common.role.ProducerInfo;
import com.mq.service.BrokerService;
import com.mq.service.LoadBalanceService;
import com.rpc.client.ClientStubProxyFactory;
import com.rpc.client.net.NettyNetClient;
import com.rpc.common.protocol.JavaSerializeMessageProtocol;
import com.rpc.common.protocol.MessageProtocol;
import com.mq.rigister.SigletonZookeeperRoleAndServiceRegister;
import com.mq.rigister.ZookeeperRoleAndServiceInfoDiscoverer;
import com.mq.rigister.ZookeeperRoleAndServiceInfoRegister;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created By xfj on 2020/3/15
 */
public class DefaultProducer {
    private volatile BlockingQueue<Message> queue;
    private ClientStubProxyFactory cspf;
    private int poolsize=4;
    ExecutorService executorService;
    private volatile ConcurrentHashMap<String,Message>sendRecord;
    private volatile CopyOnWriteArrayList<Message>ackMessage;
    private volatile CopyOnWriteArrayList<Future<List<Message>>> batchTasks;
    private volatile BlockingQueue<Message> timeOutQueue;
    private static int TIME_OUT=3000;
    private ScheduledExecutorService scheduledUpdateSubmit;
    private ScheduledExecutorService scheduledUTimeOutMessage;
    private volatile ConcurrentHashMap<String,ArrayBlockingQueue<Message>> brokerMap;
    private int queueSize;
    public DefaultProducer() {
        sendRecord=new ConcurrentHashMap<>();
        ackMessage=new CopyOnWriteArrayList<>();
        batchTasks =new CopyOnWriteArrayList<>();
        queue=new ArrayBlockingQueue<Message>(10000);
        timeOutQueue=new ArrayBlockingQueue<Message>(10000);
        brokerMap=new ConcurrentHashMap<>();
        queueSize=5;
    }

    public void init(){
        ProducerInfo producer = new ProducerInfo("producer1");
        ZookeeperRoleAndServiceInfoRegister rigister = SigletonZookeeperRoleAndServiceRegister.getInstance();
        rigister.registerRole(producer);
         cspf = new ClientStubProxyFactory();
        // 设置服务发现者
        cspf.setSid(new ZookeeperRoleAndServiceInfoDiscoverer());
        // 设置支持的协议
        Map<String, MessageProtocol> supportMessageProtocols = new HashMap<>();
        supportMessageProtocols.put("javas", new JavaSerializeMessageProtocol());
        cspf.setSupportMessageProtocols(supportMessageProtocols);
        // 设置网络层实现
        cspf.setNetClient(new NettyNetClient());
        executorService = Executors.newFixedThreadPool(poolsize);
        System.out.println("Producer初始化完毕");
    }

    //获取已执行完毕的task
    class updateSubmitTask implements Runnable{
        @Override
        public void run() {
            while (!batchTasks.isEmpty()) {
                for (Future<List<Message>> task : batchTasks) {
                    if (task.isDone()) {
                        try {
                            ackMessage.addAll(task.get());
                            batchTasks.remove(task);
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private void updateSubmit(){
        ScheduledExecutorService scheduledUpdateSubmit = Executors.newScheduledThreadPool(1);
        updateSubmitTask updateSubmitTask = new updateSubmitTask();
        scheduledUpdateSubmit.scheduleAtFixedRate(updateSubmitTask,0,200,TimeUnit.MILLISECONDS);
    }

    class updateTimeOutMessageTask implements Runnable{
        @Override
        public void run() {
            while (!sendRecord.isEmpty()) {
                //遍历sendRecord()
                for (Map.Entry<String, Message> entry : sendRecord.entrySet()) {
                    if ((System.currentTimeMillis() - entry.getValue().getTimeStamp()) > TIME_OUT && sendRecord.containsKey(entry.getKey())) {
                        System.out.println("未收到回复"+entry.getValue().getUuid());
                        timeOutQueue.offer(entry.getValue());
                        sendRecord.remove(entry.getKey());
                    }
                }
            }
        }
    }

    //根据时间戳获取超时未收到回复的message
    private void updateTimeOutMessage(){
        ScheduledExecutorService scheduledUpdateTimeOutMessage = Executors.newScheduledThreadPool(1);
        updateTimeOutMessageTask updateTimeOutMessageTask = new updateTimeOutMessageTask();
        scheduledUpdateTimeOutMessage.scheduleAtFixedRate(updateTimeOutMessageTask,0,250,TimeUnit.MILLISECONDS);
    }

    class updateSendMessageByACKTask implements Runnable{
        @Override
        public void run() {
            while (!ackMessage.isEmpty()) {
                for (Message message : ackMessage) {
                    if(sendRecord.containsKey(message.getUuid())){
                        sendRecord.remove(message.getUuid());
                        ackMessage.remove(message);
                        System.out.println("收到回复"+message.getUuid());
                    }else {
                        ackMessage.remove(message);
                        System.out.println("收到回复，未匹配"+message.getUuid());
                    }
                }
            }
        }
    }

    //根据ack更新已回复的message
    private void updateSendMessageByACK(){
        ScheduledExecutorService scheduledUpdateSendMessageByACK = Executors.newScheduledThreadPool(1);
        updateSendMessageByACKTask updateSendMessageByACKTask = new updateSendMessageByACKTask();
        scheduledUpdateSendMessageByACK.scheduleAtFixedRate(updateSendMessageByACKTask,0,300,TimeUnit.MILLISECONDS);
    }

    //retry timeOutQueue中的message,这部分用同步发送
    // TODO: 2020/3/19  retry策略，先指数再线性
    public void syncRetry(){
        //
    }

    // TODO: 2020/3/19 死信队列 持久化
    public void durableDeadMessages(){

    }

    public void start(){
        updateSubmit();
        updateTimeOutMessage();
        updateSendMessageByACK();
        init();
    }

    public List<Message> doSendBatch(List<Message> messages,String routeKey){
        if(messages.isEmpty())return new ArrayList<>();
        System.out.println("批量大小："+messages.size());
        BrokerService proxy = cspf.getProxy(BrokerService.class,routeKey);// 获取远程服务代理
        //设置时间戳，并放入发送记录
        messages.forEach(message -> {
            message.setTimeStamp(System.currentTimeMillis());
            if(message.getType()!= MessageType.ONE_WAY)
            sendRecord.put(message.getUuid(),message);
            System.out.println("发送消息"+message.getUuid()+"到broker"+routeKey);
        });
        return proxy.sendToBrokerBatch(messages);
    }


    public ArrayBlockingQueue<Message> getQueueByBrokerName(String routeKey) {
        if(brokerMap.containsKey(routeKey))
            return brokerMap.get(routeKey);
        if(!brokerMap.containsKey(routeKey))
                brokerMap.put(routeKey,new ArrayBlockingQueue<Message>(1000));
        return brokerMap.get(routeKey);
    }
    public void sendBatch(Message message,String routeKey){
        ArrayBlockingQueue<Message> queueByBrokerName = getQueueByBrokerName(routeKey);
        queueByBrokerName.offer(message);
        Future<List<Message>> submit = executorService.submit(new SendBatchTask(routeKey));
        batchTasks.add(submit);
    }

    public void sendBatch(List<Message> messages,String routeKey){
        ArrayBlockingQueue<Message> queueByBrokerName = getQueueByBrokerName(routeKey);
        for (Message message : messages) {
            queueByBrokerName.offer(message);
        }

        for(int i=0;i<messages.size() / queueSize + 1;i++) {
            Future<List<Message>> submit = executorService.submit(new SendBatchTask(routeKey));
            batchTasks.add(submit);
        }
    }

    public void sendBatch(Message message){
        if (message.getTopic() == null)
            message.setTopic(new Topic("default", 1));
        ArrayList<String> strings = new ArrayList<>();
        strings.add(message.getTopic().getTopicName());

        HashMap<String, String> map = loadBalnceByTopicNames(strings);
        sendBatch(message,map.get(message.getTopic().getTopicName()));
    }


    public void sendBatch(List<Message> messages){
        messages = messages.stream().map(message -> {
            if (message.getTopic() == null)
                message.setTopic(new Topic("default", 1));
            return message;
        }).collect(Collectors.toList());

        Map<String, List<Message>> messageGroup = messages.stream().collect(Collectors.groupingBy(message -> {
            return message.getTopic().getTopicName();
        }));

        List<String> topicNames = messageGroup.entrySet().stream().map(entry -> {
            return entry.getKey();
        }).collect(Collectors.toList());

        HashMap<String, String> topicNameBrokerMap = loadBalnceByTopicNames(topicNames);
        for (Map.Entry<String, List<Message>> entry : messageGroup.entrySet()) {
            String brokerName = topicNameBrokerMap.get(entry.getKey());
            sendBatch(entry.getValue(),brokerName);
        }
    }

    public HashMap<String,String> loadBalnceByTopicNames(List<String> tpoicNames){
        LoadBalanceService proxy = cspf.getProxy(LoadBalanceService.class);
        return proxy.loadBlanceByTpoicName(tpoicNames);
    }

    class SendBatchTask implements Callable<List<Message>>{
        String routeKey;

        public SendBatchTask(String routeKey) {
            this.routeKey = routeKey;
        }
        @Override
        public List<Message> call() throws Exception {
            List<Message> messages=new ArrayList<>(queueSize);
            ArrayBlockingQueue<Message> queue = getQueueByBrokerName(routeKey);
            queue.drainTo(messages,queueSize);
            return doSendBatch(messages,routeKey);
        }
    }
}
