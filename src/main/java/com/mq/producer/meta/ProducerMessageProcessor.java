package com.mq.producer.meta;

import com.mq.common.message.Message;
import com.mq.common.message.MessageType;
import com.mq.producer.net.RPCManager;
import com.mq.producer.router.RouteTarget;
import com.mq.producer.router.Router;
import com.mq.broker.service.BrokerService;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created By xfj on 2020/3/23
 */
public class ProducerMessageProcessor {

    ExecutorService executorService;
    private volatile ConcurrentHashMap<String, Message> sendRecord;
    private volatile CopyOnWriteArrayList<Message> ackMessage;
    private volatile CopyOnWriteArrayList<Future<List<Message>>> batchTasks;
    private volatile BlockingQueue<Message> timeOutQueue;
    private static int TIME_OUT = 3000;

    private SendQueueManager queueManager;//消息发送队列管理
    private Router router;//消息路由
    private RPCManager rpcManager;//用于获取rpc代理
    private Integer batchSize = 3;

    public ProducerMessageProcessor() {
        sendRecord = new ConcurrentHashMap<>();
        ackMessage = new CopyOnWriteArrayList<>();
        batchTasks = new CopyOnWriteArrayList<>();
        timeOutQueue = new ArrayBlockingQueue<Message>(10000);
        executorService= Executors.newFixedThreadPool(4);

        router = new Router();
        queueManager = new SendQueueManager();
        rpcManager = new RPCManager();

        updateSubmit();
        updateTimeOutMessage();
        updateSendMessageByACK();
    }

    public ProducerMessageProcessor(SendQueueManager queueManager, Router router, RPCManager rpcManager) {
        sendRecord = new ConcurrentHashMap<>();
        ackMessage = new CopyOnWriteArrayList<>();
        batchTasks = new CopyOnWriteArrayList<>();
        timeOutQueue = new ArrayBlockingQueue<Message>(10000);
        executorService= Executors.newFixedThreadPool(4);

        this.queueManager = queueManager;
        this.router = router;
        this.rpcManager = rpcManager;

        updateSubmit();
        updateTimeOutMessage();
        updateSendMessageByACK();
    }

    public ProducerMessageProcessor(SendQueueManager queueManager, Router router, RPCManager rpcManager, Integer batchSize) {
        sendRecord = new ConcurrentHashMap<>();
        ackMessage = new CopyOnWriteArrayList<>();
        batchTasks = new CopyOnWriteArrayList<>();
        timeOutQueue = new ArrayBlockingQueue<Message>(10000);
        executorService= Executors.newFixedThreadPool(4);

        this.queueManager = queueManager;
        this.router = router;
        this.rpcManager = rpcManager;
        this.batchSize = batchSize;

        updateSubmit();
        updateTimeOutMessage();
        updateSendMessageByACK();
    }

    //获取已执行完毕的task
    class UpdateSubmitTask implements Runnable {
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

    private void updateSubmit() {
        ScheduledExecutorService scheduledUpdateSubmit = Executors.newScheduledThreadPool(1);
        UpdateSubmitTask updateSubmitTask = new UpdateSubmitTask();
        scheduledUpdateSubmit.scheduleAtFixedRate(updateSubmitTask, 0, 100, TimeUnit.MILLISECONDS);
    }

    class UpdateTimeOutMessageTask implements Runnable {
        @Override
        public void run() {
            while (!sendRecord.isEmpty()) {
                //遍历sendRecord()
                for (Map.Entry<String, Message> entry : sendRecord.entrySet()) {
                    if ((System.currentTimeMillis() - entry.getValue().getTimeStamp()) > TIME_OUT && sendRecord.containsKey(entry.getKey())) {
                        System.out.println("未收到回复" + entry.getValue().getUuid());
                        timeOutQueue.offer(entry.getValue());
                        sendRecord.remove(entry.getKey());
                    }
                }
            }
        }
    }

    //根据时间戳获取超时未收到回复的message
    private void updateTimeOutMessage() {
        ScheduledExecutorService scheduledUpdateTimeOutMessage = Executors.newScheduledThreadPool(1);
        UpdateTimeOutMessageTask updateTimeOutMessageTask = new UpdateTimeOutMessageTask();
        scheduledUpdateTimeOutMessage.scheduleAtFixedRate(updateTimeOutMessageTask, 0, 100, TimeUnit.MILLISECONDS);
    }

    class UpdateSendMessageByACKTask implements Runnable {
        @Override
        public void run() {
            while (!ackMessage.isEmpty()) {
                for (Message message : ackMessage) {
                    if (sendRecord.containsKey(message.getUuid())) {
                        sendRecord.remove(message.getUuid());
                        ackMessage.remove(message);
                        System.out.println("收到回复" + message.getUuid());
                    } else {
                        ackMessage.remove(message);
                        System.out.println("收到回复，未匹配" + message.getUuid());
                    }
                }
            }
        }
    }

    //根据ack更新已回复的message
    private void updateSendMessageByACK() {
        ScheduledExecutorService scheduledUpdateSendMessageByACK = Executors.newScheduledThreadPool(1);
        UpdateSendMessageByACKTask updateSendMessageByACKTask = new UpdateSendMessageByACKTask();
        scheduledUpdateSendMessageByACK.scheduleAtFixedRate(updateSendMessageByACKTask, 0, 100, TimeUnit.MILLISECONDS);
    }

    //retry timeOutQueue中的message,这部分用同步发送
    // TODO: 2020/3/19  retry策略，先指数再线性
    public void syncRetry() {
        //
    }

    // TODO: 2020/3/19 死信队列 持久化
    public void durableDeadMessages() {

    }


    public List<Message> doSendBatch(List<Message> messages,  RouteTarget routeTarget) {
        if (messages.isEmpty()) return new ArrayList<>();
        System.out.println("批量大小：" + messages.size());

        //broker作为key传入
        BrokerService rpcProxy = rpcManager.getRPCProxy(BrokerService.class, routeTarget.getBrokerName());// 获取远程服务代理

        //设置时间戳，并放入发送记录
        messages.forEach(message -> {
            message.setTimeStamp(System.currentTimeMillis());
            if (message.getType() != MessageType.ONE_WAY)
                sendRecord.put(message.getUuid(), message);
            System.out.println("发送消息" + message.getUuid() + "到broker" + routeTarget.getBrokerName());
        });
        //这里的queue传参的形式传入
        return rpcProxy.sendToBrokerBatch(messages, routeTarget.getQueueIndex());
    }


    public void sendBatch(List<Message> messages, RouteTarget routeTarget) {
        //获取队列，向队列offer消息
        ArrayBlockingQueue queue = queueManager.getQueue(routeTarget.getRouteKey());
        for (Message message : messages) {
            queue.offer(message);
        }

        //每次发送一个batch的数据
        for (int i = 0; i < messages.size() / batchSize + 1; i++) {
            Future<List<Message>> submit = executorService.submit(new SendBatchTask(routeTarget));
            batchTasks.add(submit);
        }
    }




    public void sendBatch(List<Message> messages) {
        //路由，并根据路由分组
        HashMap<RouteTarget, List<Message>> routeTargetListHashMap = new HashMap<>();
        messages.forEach(message -> {
            RouteTarget routeTarget = router.routeMessage(message);
            if(routeTargetListHashMap.containsKey(routeTarget)){
                routeTargetListHashMap.get(routeTarget).add(message);
            }else{
                ArrayList<Message> messages1 = new ArrayList<>();
                messages1.add(message);
                routeTargetListHashMap.put(routeTarget,messages1);
            }
        });
        for (Map.Entry<RouteTarget, List<Message>> routeTargetListEntry : routeTargetListHashMap.entrySet()) {
            sendBatch(routeTargetListEntry.getValue(), routeTargetListEntry.getKey());
        }
    }

    class SendBatchTask implements Callable<List<Message>> {
        RouteTarget routeTarget;

        public SendBatchTask(RouteTarget routeTarget) {
            this.routeTarget = routeTarget;
        }

        @Override
        public List<Message> call() throws Exception {
            List<Message> messages = new ArrayList<>(batchSize);
            ArrayBlockingQueue queue = queueManager.getQueue(routeTarget.getRouteKey());
            queue.drainTo(messages, batchSize);
            return doSendBatch(messages, routeTarget);
        }
    }

}
