package com.rpc.demo.provider;

import com.rpc.common.protocol.JavaSerializeMessageProtocol;
import com.rpc.demo.DemoService;
import com.rpc.server.NettyRpcServer;
import com.rpc.server.RequestHandler;
import com.rpc.server.RpcServer;
import com.rpc.server.register.ServiceObject;
import com.rpc.server.register.ServiceRegister;
import com.rpc.server.register.ZookeeperExportServiceRegister;
import com.rpc.util.PropertiesUtils;

public class Provider {
	public static void main(String[] args) throws Exception {

		int port = Integer.parseInt(PropertiesUtils.getProperties("rpc.port"));
		String protocol = PropertiesUtils.getProperties("rpc.protocol");

		// 服务注册
		ServiceRegister sr = new ZookeeperExportServiceRegister();
		//new 具体的实现
		DemoService ds = new DemoServiceImpl();
		ServiceObject so = new ServiceObject(DemoService.class.getName(), DemoService.class, ds);
		sr.register(so, protocol, port);

		RequestHandler reqHandler = new RequestHandler(new JavaSerializeMessageProtocol(), sr);

		RpcServer server = new NettyRpcServer(port, protocol, reqHandler);
		server.start();
		System.in.read(); // 按任意键退出
		server.stop();
	}
}
