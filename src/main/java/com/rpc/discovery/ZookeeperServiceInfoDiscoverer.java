package com.rpc.discovery;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.ZkClient;

import com.alibaba.fastjson.JSON;

public class ZookeeperServiceInfoDiscoverer implements ServiceInfoDiscoverer {

	ZkClient client;

	private String centerRootPath = "/rpc";

	public ZookeeperServiceInfoDiscoverer() {
		String addr = PropertiesUtils.getProperties("zk.address");
		client = new ZkClient(addr);
		client.setZkSerializer(new MyZkSerializer());
	}

	//zk取节点，返回对应的ServiceInfo
	@Override
	public List<ServiceInfo> getServiceInfo(String name) {
		String servicePath = centerRootPath + "/" + name + "/service";
		List<String> children = client.getChildren(servicePath);
		List<ServiceInfo> resources = new ArrayList<ServiceInfo>();
		for (String ch : children) {
			try {
				String deCh = URLDecoder.decode(ch, "UTF-8");
				ServiceInfo r = JSON.parseObject(deCh, ServiceInfo.class);
				resources.add(r);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return resources;
	}

}
