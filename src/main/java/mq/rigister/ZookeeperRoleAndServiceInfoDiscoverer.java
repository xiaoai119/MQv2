package mq.rigister;

import com.alibaba.fastjson.JSON;
import com.rpc.discovery.ServiceInfo;
import com.rpc.discovery.ServiceInfoDiscoverer;
import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import mq.common.role.BrokerInfo;
import mq.common.role.RoleInfo;
import mq.service.BrokerService;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created By xfj on 2020/3/16
 * 根据服务名发现role
 */
public class ZookeeperRoleAndServiceInfoDiscoverer implements ServiceInfoDiscoverer {
    ZkClient client;

    private String centerRootPath = "/mq";

    public ZookeeperRoleAndServiceInfoDiscoverer() {
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
        String servicePath= rolePath+"/"+roleName+"/"+"service";
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

    //根据service名获取对应的brokerInfo列表
    public <T> List<T> getRoleInfo(String name,Class<T> role){
        String servicePath = centerRootPath + "/" + name;
        List<String> children = client.getChildren(servicePath);
        List<T> resources = new ArrayList<>();
        for (String ch : children) {
            try {
                String deCh = URLDecoder.decode(ch, "UTF-8");
                T r = JSON.parseObject(deCh, role);
                resources.add(r);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return resources;
    }

    //服务发现，根据routeKey获取对应的的service列表
        public List<ServiceInfo> getServiceInfo(String name,String routeKey){
//        先根据serviceName获取roleInfo的list
        String rolePath = centerRootPath + "/" + name;
        List<String> children = client.getChildren(rolePath);
        List<ServiceInfo> result = new ArrayList<ServiceInfo>();
        for (String ch : children) {
            try {
                String deCh = URLDecoder.decode(ch, "UTF-8");
                RoleInfo r = JSON.parseObject(deCh, RoleInfo.class);
                //根据routeKey获取服务
                if(r.getName().equals(routeKey)){
                    String servicePath= rolePath+"/"+ch+"/"+"service";
                    List<String> children1 = client.getChildren(servicePath);
                    for(String ch1:children1){
                        String deCh1 = URLDecoder.decode(ch1, "UTF-8");
                        ServiceInfo serviceInfo = JSON.parseObject(deCh1, ServiceInfo.class);
                        result.add(serviceInfo);
                    }
                    break;
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        //遍历
        return result;
    }

    public static void main(String[] args) {
        ZookeeperRoleAndServiceInfoDiscoverer zookeeperRoleInfoDiscoverer = new ZookeeperRoleAndServiceInfoDiscoverer();
        List<ServiceInfo> broker1 = zookeeperRoleInfoDiscoverer.getServiceInfo(BrokerService.class.getName(), "broker1");
        System.out.println(broker1);
    }
}
