package com.mq.common.role;

import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/6/10
 * broker的consumermanager中的消费信息
 * consumer名称以及queueIndex
 */
public class ConsumeInfo {
    String consumerName;
    List<Integer> queues;
    public ConsumeInfo(String consumerName) {
        this.consumerName = consumerName;
        queues=new ArrayList<Integer>();
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public List<Integer> getQueues() {
        return queues;
    }

    public void setQueues(List<Integer> queues) {
        this.queues = queues;
    }
}
