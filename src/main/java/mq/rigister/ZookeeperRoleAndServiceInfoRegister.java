package mq.rigister;

import com.alibaba.fastjson.JSON;
import com.rpc.discovery.ServiceInfo;
import com.rpc.server.register.*;
import com.rpc.util.PropertiesUtils;
import mq.common.role.BrokerInfo;
import mq.common.role.RoleInfo;
import org.I0Itec.zkclient.ZkClient;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created By xfj on 2020/3/15
 */
public class ZookeeperRoleAndServiceInfoRegister extends DefaultServiceRegister implements ServiceRegister {
    ZkClient client;
    private String centerRootPath = "/mq";

    public ZookeeperRoleAndServiceInfoRegister() {
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
    }

    public void registerRole(RoleInfo roleInfo){
        getRoleUri(roleInfo);
    }

    public void registerRole(RoleInfo roleInfo, List<ServiceObject> sos , String protocolName){
        //暴露该角色
        String roleUri = getRoleUri(roleInfo);
        for(ServiceObject so:sos) {
            ServiceInfo serviceInfo = registerService(so, protocolName, roleInfo.getPort());
            exportService(serviceInfo,roleUri);
        }
    }


    private String getRoleUri(RoleInfo roleInfo) {
            String uri = JSON.toJSONString(roleInfo);
        try {
            uri = URLEncoder.encode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return uri;
    }

    public ServiceInfo registerService(ServiceObject so, String protocolName,int port){
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

    private void exportService(ServiceInfo serviceResource,String roleUri) {

        String serviceName = serviceResource.getName();
        String uri = JSON.toJSONString(serviceResource);
        try {
            uri = URLEncoder.encode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String servicePath = centerRootPath + "/" + serviceName +"/"+ roleUri + "/service";
        if (!client.exists(servicePath)) {
            client.createPersistent(servicePath, true);
        }
        String uriPath = servicePath  + "/" + uri;
        if (client.exists(uriPath)) {
            client.delete(uriPath);
        }
        // TODO: 2020/3/19 先改成永久的用于调试
        client.createPersistent(uriPath);
    }

}
