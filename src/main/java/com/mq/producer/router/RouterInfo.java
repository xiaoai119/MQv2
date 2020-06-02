package com.mq.producer.router;

import com.mq.common.message.Message;

/**
 * Created By xfj on 2020/5/9
 */
public class RouterInfo {
    private RouteTarget routeTarget;
    private Message message;

    public RouterInfo(RouteTarget routeTarget, Message message) {
        this.routeTarget = routeTarget;
        this.message = message;
    }

    public RouteTarget getRouteTarget() {
        return routeTarget;
    }

    public void setRouteTarget(RouteTarget routeTarget) {
        this.routeTarget = routeTarget;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
