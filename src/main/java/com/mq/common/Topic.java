package com.mq.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/2/5
 */
public class Topic implements Serializable{
	private static final long serialVersionUID = -3115497946567476212L;

	String topicName;
    int queueNum;

	public Topic(String s) {
		topicName = s;
		queueNum=1;
	}

    public Topic(String topicName, int queueNum) {
        this.topicName = topicName;
        this.queueNum = queueNum;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getQueueNum() {
        return queueNum;
    }

    public void setQueueNum(Integer queueNum) {
        this.queueNum = queueNum;
    }

    public String getTopicName() {
		return topicName;
	}
}
