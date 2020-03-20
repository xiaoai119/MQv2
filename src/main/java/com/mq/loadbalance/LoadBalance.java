package com.mq.loadbalance;

import com.rpc.client.ClientStubProxyFactory;
import com.rpc.client.net.NettyNetClient;
import com.rpc.common.protocol.JavaSerializeMessageProtocol;
import com.rpc.common.protocol.MessageProtocol;
import com.rpc.server.NettyRpcServer;
import com.rpc.server.RequestHandler;
import com.rpc.server.RpcServer;
import com.rpc.server.register.ServiceObject;
import com.rpc.util.PropertiesUtils;
import com.mq.common.role.BrokerInfo;
import com.mq.common.role.LoadBalanceInfo;
import com.mq.rigister.SigletonZookeeperRoleAndServiceRegister;
import com.mq.rigister.ZookeeperRoleAndServiceInfoDiscoverer;
import com.mq.rigister.ZookeeperRoleAndServiceInfoRegister;
import com.mq.service.BrokerService;
import com.mq.service.LoadBalanceServiceImpl;
import com.mq.service.LoadBalanceService;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created By xfj on 2020/3/16
 * 负载均衡类
 */
public class LoadBalance {
    ConcurrentHashMap<String, String> routeMap;//本地路由表<topicName,brokerName>
    LoadBalanceInfo loadBalanceInfo;
    ZookeeperRoleAndServiceInfoDiscoverer zookeeperRoleInfoDiscoverer;
    private Random random;
    ClientStubProxyFactory cspf;

    public LoadBalance(LoadBalanceInfo loadBalanceInfo) {
        this.routeMap = new ConcurrentHashMap<>();
        this.loadBalanceInfo = loadBalanceInfo;
        zookeeperRoleInfoDiscoverer = new ZookeeperRoleAndServiceInfoDiscoverer();
        random = new Random();
    }


    public void init() {
        ZookeeperRoleAndServiceInfoRegister rigister = SigletonZookeeperRoleAndServiceRegister.getInstance();
        String protocol = PropertiesUtils.getProperties("rpc.protocol");
        LoadBalanceServiceImpl loadBalanceService = new LoadBalanceServiceImpl(this);
        ServiceObject so = new ServiceObject(LoadBalanceService.class.getName(), LoadBalanceService.class, loadBalanceService);
        ArrayList<ServiceObject> serviceObjects = new ArrayList<>();
        serviceObjects.add(so);
        rigister.registerRole(loadBalanceInfo, serviceObjects, protocol);
        RequestHandler reqHandler = new RequestHandler(new JavaSerializeMessageProtocol(), rigister);
        RpcServer server = new NettyRpcServer(loadBalanceInfo.getPort(), protocol, reqHandler);

        cspf = new ClientStubProxyFactory();
        // 设置服务发现者
        cspf.setSid(new ZookeeperRoleAndServiceInfoDiscoverer());
        // 设置支持的协议
        Map<String, MessageProtocol> supportMessageProtocols = new HashMap<>();
        supportMessageProtocols.put("javas", new JavaSerializeMessageProtocol());
        cspf.setSupportMessageProtocols(supportMessageProtocols);
        // 设置网络层实现
        cspf.setNetClient(new NettyNetClient());
        server.start();
    }

    public void start() {
        init();
    }

    public List<BrokerInfo> brokerDiscover(String name) {
        return zookeeperRoleInfoDiscoverer.getRoleInfo(name, BrokerInfo.class);
    }

    public HashMap<String, String> loadBlanceByTpoicNames(List<String> topicNames) {
        HashMap<String, String> result = new HashMap<>();
        List<BrokerInfo> brokerInfos = brokerDiscover(BrokerService.class.getName());

        for (String topicName : topicNames) {
            //对于每一个key，首先查找本地的路由表
            if (routeMap.containsKey(topicName)) {
                result.put(topicName, routeMap.get(topicName));
            } else {
                String brokerName = brokerInfos.get(random.nextInt(brokerInfos.size())).getName();
                result.put(topicName, brokerName);
            }
        }
        return result;
    }
}
