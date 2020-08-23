package com.rpc.client.net;

import java.nio.charset.Charset;
import java.time.LocalTime;
import java.util.concurrent.CountDownLatch;

import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rpc.discovery.ServiceInfo;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NettyNetClient implements NetClient {

	private static Logger logger = LoggerFactory.getLogger(NettyNetClient.class);

	@Override
	public byte[] sendRequest(byte[] data, ServiceInfo sinfo) throws Throwable {

		String[] addInfoArray = sinfo.getAddress().split(":");

		final SendHandler sendHandler = new SendHandler(data);
		byte[] respData = null;
		// 配置客户端
		EventLoopGroup group = new NioEventLoopGroup(3);
		try {
			Bootstrap b = new Bootstrap();

			b.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
					.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(65535))
					.handler(new ChannelInitializer<SocketChannel>() {
						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ChannelPipeline p = ch.pipeline();
							p.addLast(sendHandler);
							p.addLast(new HeartbeatHandler());
						}
					});

			// 启动客户端连接
			b.connect(addInfoArray[0], Integer.valueOf(addInfoArray[1])).sync();
			respData = (byte[]) sendHandler.rspData();
			logger.info("sendRequest get reply: " + respData);
		} finally {
			// 释放线程组资源
			group.shutdownGracefully();
		}

		return respData;
	}



	private class SendHandler extends ChannelInboundHandlerAdapter {

		private CountDownLatch cdl = null;
		private Object readMsg = null;
		private byte[] data;

		public SendHandler(byte[] data) {
			cdl = new CountDownLatch(1);
			this.data = data;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			logger.info("连接服务端成功：" + ctx);
			ByteBuf reqBuf = Unpooled.buffer(data.length);
			reqBuf.writeBytes(data);
			logger.info("客户端发送消息：" + reqBuf);
			ctx.writeAndFlush(reqBuf);
		}

		public Object rspData() {

			try {
				//未返回之前阻塞
				cdl.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			return readMsg;
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			logger.info("client read msg: " + msg);
			ByteBuf msgBuf = (ByteBuf) msg;
			byte[] resp = new byte[msgBuf.readableBytes()];
			msgBuf.readBytes(resp);
			readMsg = resp;
			cdl.countDown();
		}

		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) {
			ctx.flush();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			// Close the connection when an exception is raised.
			cause.printStackTrace();
			logger.error("发生异常：" + cause.getMessage());
			ctx.close();
		}
	}

	public class HeartbeatHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof IdleStateEvent) {
				IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
				if (idleStateEvent.state() == IdleState.WRITER_IDLE) {
					System.out.println("10秒了，需要发送消息给服务端了" + LocalTime.now());
					//向服务端送心跳包
					ByteBuf buffer = getByteBuf(ctx);
					//发送心跳消息，并在发送失败时关闭该连接
					ctx.writeAndFlush(buffer).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
				}
			} else {
				super.userEventTriggered(ctx, evt);
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			System.out.println("捕获的异常：" + cause.getMessage());
			ctx.channel().close();
		}

		private ByteBuf getByteBuf(ChannelHandlerContext ctx) {
			// 1. 获取二进制抽象 ByteBuf
			ByteBuf buffer = ctx.alloc().buffer();
			String time = "heartbeat:客户端心跳数据：" + LocalTime.now();
			// 2. 准备数据，指定字符串的字符集为 utf-8
			byte[] bytes = time.getBytes(Charset.forName("utf-8"));
			// 3. 填充数据到 ByteBuf
			buffer.writeBytes(bytes);
			return buffer;
		}
	}
}
