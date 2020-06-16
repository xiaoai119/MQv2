package com.mq.broker.net;

import com.alibaba.fastjson.JSON;
import com.rpc.discovery.ServiceInfo;
import com.rpc.discovery.ServiceInfoDiscoverer;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/6/3
 */
public class ConsumerServiceDiscover implements ServiceInfoDiscoverer {
    ZkClient client;

    private String centerRootPath = "/com/mq";

    public ConsumerServiceDiscover() {
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
    }


    //不指定roleName,返回默认role的service
    @Override
    public List<ServiceInfo> getServiceInfo(String name) {
        ArrayList<ServiceInfo> serviceInfos = new ArrayList<>();
        String rolePath = centerRootPath + "/" + name;
        List<String> children = client.getChildren(rolePath);
        //默认取第一个节点
        if(children.isEmpty())return new ArrayList<>();
        String roleName =children.get(0);
        String servicePath= rolePath+"/"+roleName+"/";
        List<String> children1 = client.getChildren(servicePath);
        for(String ch1:children1){
            try {
                String deCh1 = URLDecoder.decode(ch1, "UTF-8");
                ServiceInfo serviceInfo = JSON.parseObject(deCh1, ServiceInfo.class);
                serviceInfos.add(serviceInfo);
            }catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return serviceInfos;
    }

    //服务发现，根据routeKey获取对应的的service列表
    public List<ServiceInfo> getServiceInfo(String name,String routeKey){
//        先根据serviceName获取roleInfo的list
        String rolePath = centerRootPath + "/" + name;
        List<String> children = client.getChildren(rolePath);
        List<ServiceInfo> result = new ArrayList<ServiceInfo>();
        for (String ch : children) {
            try {
//                String deCh = URLDecoder.decode(ch, "UTF-8");
//                RoleMetaInfo r = JSON.parseObject(deCh, RoleMetaInfo.class);
                //根据routeKey获取服务
                if(ch.equals(routeKey)){
                    String servicePath= rolePath+"/"+ch;
                    List<String> children1 = client.getChildren(servicePath);
                    for(String ch1:children1){
                        String deCh1 = URLDecoder.decode(ch1, "UTF-8");
                        ServiceInfo serviceInfo = JSON.parseObject(deCh1, ServiceInfo.class);
                        result.add(serviceInfo);
                    }
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
