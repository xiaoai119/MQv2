package com.mq.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created By xfj on 2020/2/5
 */
public class Topic implements Serializable{
	private static final long serialVersionUID = -3115497946567476212L;

	private String topicName;//topic名称
    int queueNum;//每个包含的队列数
    HashMap<String,List<Integer>> queues;//第一个string是broker名，第二个是队列id

	public Topic(String s) {
		topicName = s;
		queueNum=1;
        queues=new HashMap<>();
	}

    public Topic(String topicName, int queueNum) {
        this.topicName = topicName;
        this.queueNum = queueNum;
        queues=new HashMap<>();
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getQueueNum() {
        return queueNum;
    }

    public void setQueueNum(int queueNum) {
        this.queueNum = queueNum;
    }

    public HashMap<String, List<Integer>> getQueues() {
        return queues;
    }

    public void setQueues(HashMap<String, List<Integer>> queues) {
        this.queues = queues;
    }

    @Override
    public int hashCode() {
        return topicName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Topic other = (Topic) obj;
        if (topicName == null) {
            if (other.topicName != null)
                return false;
        }
        return topicName.equals(other.topicName);
    }

}
