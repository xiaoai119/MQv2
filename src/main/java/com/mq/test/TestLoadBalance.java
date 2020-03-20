package com.mq.test;

import com.mq.common.role.LoadBalanceInfo;
import com.mq.loadbalance.LoadBalance;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created By xfj on 2020/3/19
 */
public class TestLoadBalance {
    public static void main(String[] args) throws UnknownHostException {
        LoadBalanceInfo loadBalanceInfo = new LoadBalanceInfo(InetAddress.getLocalHost().getHostAddress(), 8119,"loadbalance");
        LoadBalance loadBalance = new LoadBalance(loadBalanceInfo);
        loadBalance.start();

    }
}
