package com.mq.consumer.common;

import java.util.List;

/**
 * Created By xfj on 2020/6/12
 */
public class AllocateUnit {
    String topicName;
    String brokerName;
    List<Integer> queues;

    public AllocateUnit(String topicName, String brokerName, List<Integer> queues) {
        this.topicName = topicName;
        this.brokerName = brokerName;
        this.queues = queues;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public List<Integer> getQueues() {
        return queues;
    }

    public void setQueues(List<Integer> queues) {
        this.queues = queues;
    }
}
