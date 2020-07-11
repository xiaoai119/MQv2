package com.mq.broker.rigister;

import com.alibaba.fastjson.JSON;
import com.mq.common.Topic;
import com.mq.common.role.BrokerMetaInfo;
import com.rpc.discovery.ServiceInfo;
import com.rpc.server.register.DefaultServiceRegister;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.server.register.ServiceObject;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created By xfj on 2020/3/23
 */
public class ZookeeperBrokerRegister extends DefaultServiceRegister {
    ZkClient client;
    private String centerRootPath = "/com/mq";

    public ZookeeperBrokerRegister() {
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
    }

    public void registerBroker(BrokerMetaInfo brokerMetaInfo) {
        String brokerMetaUri = getBrokerMetaUri(brokerMetaInfo);
        String servicePath = centerRootPath + "/" + "broker" + "/" + brokerMetaInfo.getName();
        if (!client.exists(servicePath)) {
            client.createPersistent(servicePath, true);
        }
        String uriPath = servicePath + "/" + brokerMetaUri;
        if (client.exists(uriPath)) {
            client.delete(uriPath);
        }
        client.createEphemeral(uriPath);
    }

    public void registerBrokerAndService(BrokerMetaInfo brokerMetaInfo, List<ServiceObject> sos, String protocolName) {
        //暴露该角色
        registerBroker(brokerMetaInfo);

        for (ServiceObject so : sos) {
            ServiceInfo serviceInfo = registerService(so, protocolName, brokerMetaInfo.getPort());
            exportService(serviceInfo, brokerMetaInfo.getName());
        }
    }


    private String getBrokerMetaUri(BrokerMetaInfo brokerMetaInfo) {
        String uri = JSON.toJSONString(brokerMetaInfo);
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

    private void exportService(ServiceInfo serviceResource, String brokerName) {

        String serviceName = serviceResource.getName();
        String uri = JSON.toJSONString(serviceResource);
        try {
            uri = URLEncoder.encode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        // TODO: 2020/6/2 这里先不加brokername
        String servicePath = centerRootPath + "/" + serviceName + "/" + brokerName;
//        String servicePath = centerRootPath + "/" + serviceName;
        if (!client.exists(servicePath)) {
            client.createPersistent(servicePath, true);
        }
        String uriPath = servicePath + "/" + uri;
        if (client.exists(uriPath)) {
            client.delete(uriPath);
        }
        client.createEphemeral(uriPath);
    }


}
