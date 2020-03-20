package com.rpc.server.register;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;

import com.rpc.discovery.ServiceInfo;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import com.alibaba.fastjson.JSON;

/**
 * Zookeeper方式获取远程服务信息类。
 * 
 * ZookeeperServiceInfoDiscoverer
 */
public class ZookeeperExportServiceRegister extends DefaultServiceRegister implements ServiceRegister {

	private ZkClient client;

	private String centerRootPath = "/rpc";

	public ZookeeperExportServiceRegister() {
		String addr = PropertiesUtils.getProperties("zk.address");
		client = new ZkClient(addr);
		client.setZkSerializer(new MyZkSerializer());
	}

	@Override
	public void register(ServiceObject so, String protocolName, int port) {
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
		exportService(soInf);
	}

	private void exportService(ServiceInfo serviceResource) {
		String serviceName = serviceResource.getName();
		String uri = JSON.toJSONString(serviceResource);
		try {
			uri = URLEncoder.encode(uri, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		String servicePath = centerRootPath + "/" + serviceName + "/service";
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
