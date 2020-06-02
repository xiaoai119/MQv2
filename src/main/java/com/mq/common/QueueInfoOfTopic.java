package com.mq.common;

import java.util.List;

/**
 * Created By xfj on 2020/6/1
 */
public class QueueInfoOfTopic {
    String topicName;
    String brokerName;
    List<Integer> queueIndex;

    public QueueInfoOfTopic() {}

    public QueueInfoOfTopic(String topicName, String brokerName, List<Integer> queueIndex) {
        this.topicName = topicName;
        this.brokerName = brokerName;
        this.queueIndex = queueIndex;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public List<Integer> getQueueIndex() {
        return queueIndex;
    }

    public void setQueueIndex(List<Integer> queueIndex) {
        this.queueIndex = queueIndex;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
}
