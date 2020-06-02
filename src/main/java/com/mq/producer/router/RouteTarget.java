package com.mq.producer.router;

import com.mq.common.Topic;

/**
 * Created By xfj on 2020/3/23
 */
public class RouteTarget {
    String brokerName;
    Integer queueIndex;

    public RouteTarget(String brokerName, Integer queueIndex) {
        this.brokerName = brokerName;
        this.queueIndex = queueIndex;
    }

    public String getRouteKey(){
        return brokerName+queueIndex;
    }

    @Override
    public int hashCode() {
        return (brokerName+queueIndex).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RouteTarget other = (RouteTarget) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        }
        return (brokerName+queueIndex).equals(other.brokerName+other.queueIndex);
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Integer getQueueIndex() {
        return queueIndex;
    }

    public void setQueueIndex(Integer queueIndex) {
        this.queueIndex = queueIndex;
    }
}
