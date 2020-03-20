package mq.broker;

import com.rpc.common.protocol.JavaSerializeMessageProtocol;
import com.rpc.server.NettyRpcServer;
import com.rpc.server.RequestHandler;
import com.rpc.server.RpcServer;
import com.rpc.server.register.ServiceObject;
import com.rpc.util.PropertiesUtils;
import mq.common.message.Message;
import mq.common.message.MessageType;
import mq.common.role.BrokerInfo;
import mq.common.IpNode;
import mq.common.Topic;
import mq.rigister.SigletonZookeeperRoleAndServiceRegister;
import mq.rigister.ZookeeperRoleAndServiceInfoRegister;
import mq.service.BrokerService;
import mq.service.BrokerServiceImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created By xfj on 2020/3/15
 */
public class Broker {
    private volatile int count = 0;//记录队列编号
    private int pushTime = 1000;//push时间默认一秒一次
    private boolean hasQueueNum = false;//是否指定队列数
    private int retryTime = 16;//发送失败重试次数
    private ConcurrentHashMap<Integer,MyQueue> queueMap;//队列
    private IpNode node;//broker服务端的端口
    private HashMap<String,Topic> topicMap;
    private BrokerInfo brokerInfo;
    public Broker(BrokerInfo brokerInfo){
        this.brokerInfo=brokerInfo;
        queueMap=new ConcurrentHashMap<Integer, MyQueue>();
    }

    public void init() {
        ZookeeperRoleAndServiceInfoRegister rigister = SigletonZookeeperRoleAndServiceRegister.getInstance();
        String protocol = PropertiesUtils.getProperties("rpc.protocol");
        BrokerService brokerService = new BrokerServiceImpl(this);
        ServiceObject so = new ServiceObject(BrokerService.class.getName(), BrokerService.class, brokerService);
        ArrayList<ServiceObject> serviceObjects = new ArrayList<>();
        serviceObjects.add(so);
        rigister.registerRole(brokerInfo,serviceObjects,protocol);
        RequestHandler reqHandler = new RequestHandler(new JavaSerializeMessageProtocol(), rigister);
        RpcServer server = new NettyRpcServer(brokerInfo.getPort(), protocol, reqHandler);
        server.start();
    }

    public void start(){
        init();
    }

    public boolean  addToBroker(Message message){
        LoadBalance loadBalance = new LoadBalance(this);
        List<Integer> balance = loadBalance.balance(message);
        for(Integer i:balance) {
            this.add(i, message);
            // TODO: 2020/3/19  测试用
            System.out.println("添加消息"+message.getMessage()+"到队列"+i);
        }


        return true;
    }

    public Message addAndReply(Message message){
        if(addToBroker(message)){
            Message ack = new Message("ACK", MessageType.ACK, message.getNum());
            ack.setUuid(message.getUuid());
            return ack;
        }
        return null;
    }

    public synchronized void add(int queueNumber, Message value) {
        MyQueue queue = queueMap.get(queueNumber);
        queue.putAtHeader(value);
    }

    public static void main(String[] args) {
        BrokerInfo brokerInfo = new BrokerInfo("127.0.0.1", 8118,"broker1",new ArrayList<String>());
        Broker broker = new Broker(brokerInfo);
        broker.start();
    }

    public ConcurrentHashMap<Integer, MyQueue> getQueueMap() {
        return queueMap;
    }
}
