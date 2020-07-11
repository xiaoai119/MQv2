package com.mq.consumer.net;

import com.alibaba.fastjson.JSON;

import com.mq.common.Topic;
import com.mq.common.role.ConsumerMetaInfo;
import com.rpc.discovery.ServiceInfo;
import com.rpc.server.register.DefaultServiceRegister;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.server.register.ServiceObject;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created By xfj on 2020/6/3
 * 注册信息
 */
public class ZookeeperConsumerRigister extends DefaultServiceRegister {
    ZkClient client;
    private String centerRootPath = "/com/mq";

    public ZookeeperConsumerRigister() {
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
    }


    //
    public void registerConsumer(ConsumerMetaInfo consumerMetaInfo) {
        String consumerMetaUri = getConsumerMetaUri(consumerMetaInfo);
        String servicePath = centerRootPath + "/" + "consumer" + "/" + consumerMetaInfo.getName();
        if (!client.exists(servicePath)) {
            client.createPersistent(servicePath, true);
        }
//        String uriPath = servicePath + "/" + consumerMetaUri;
//        if (client.exists(uriPath)) {
//            client.delete(uriPath);
//        }
//        client.createEphemeral(uriPath);
    }

    public void registerConsumerAndService(ConsumerMetaInfo consumerMetaInfo, List<ServiceObject> sos, String protocolName) {
        //暴露该角色
        registerConsumer(consumerMetaInfo);

        for (ServiceObject so : sos) {
            ServiceInfo serviceInfo = registerService(so, protocolName, consumerMetaInfo.getPort());
            exportService(serviceInfo, consumerMetaInfo.getName());
        }
    }


    private String getConsumerMetaUri(ConsumerMetaInfo consumerMetaInfo) {
        String uri = JSON.toJSONString(consumerMetaInfo);
        try {
            uri = URLEncoder.encode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return uri;
    }

    public ServiceInfo registerService(ServiceObject so, String protocolName, int port) {
        try {
            super.register(so, protocolName, port);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ServiceInfo soInf = new ServiceInfo();

        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        String address = host + ":" + port;
        soInf.setAddress(address);
        soInf.setName(so.getInterf().getName());
        soInf.setProtocol(protocolName);
        return soInf;
    }

    private void exportService(ServiceInfo serviceResource, String consumerName) {

        String serviceName = serviceResource.getName();
        String uri = JSON.toJSONString(serviceResource);
        try {
            uri = URLEncoder.encode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String servicePath = centerRootPath + "/" + serviceName + "/" + consumerName;
        if (!client.exists(servicePath)) {
            client.createPersistent(servicePath, true);
        }
        String uriPath = servicePath + "/" + uri;
        if (client.exists(uriPath)) {
            client.delete(uriPath);
        }
        client.createEphemeral(uriPath);
    }



    public static void main(String[] args) throws UnknownHostException {
        ConsumerMetaInfo consumerMetaInfo = new ConsumerMetaInfo(InetAddress.getLocalHost().getHostAddress(), 8118,"consumer1");


        Topic testTopic = new Topic("testTopic3",10);

        ZookeeperConsumerRigister zookeeperConsumerRigister = new ZookeeperConsumerRigister();
        zookeeperConsumerRigister.registerConsumer(consumerMetaInfo);



        System.out.println();
    }
}
