//package com.mq.test;
//
//import com.mq.common.role.LoadBalanceMetaInfo;
//import com.mq.loadbalance.LoadBalance;
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//
///**
// * Created By xfj on 2020/3/19
// */
//public class TestLoadBalance {
//    public static void main(String[] args) throws UnknownHostException {
//        LoadBalanceMetaInfo loadBalanceInfo = new LoadBalanceMetaInfo(InetAddress.getLocalHost().getHostAddress(), 8120,"loadbalance");
//        LoadBalance loadBalance = new LoadBalance(loadBalanceInfo);
//        loadBalance.start();
//
//    }
//}
