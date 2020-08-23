package com.mq.zklock;

import com.rpc.server.register.MyZkSerializer;
import com.rpc.util.PropertiesUtils;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Created By xfj on 2020/7/17
 */
public class ZooKeeperDistributionLock implements Lock{

    /*
     * 利用临时顺序节点来实现分布式锁
     * 获取锁：取排队号（创建自己的临时顺序节点），然后判断自己是否是最小号，如是，则获得锁；不是，则注册前一节点的watcher,阻塞等待
     * 释放锁：删除自己创建的临时顺序节点
     */
    private String LockPath;

    private ZkClient client;

    private String currentPath;

    private String beforePath;

    public ZooKeeperDistributionLock(String lockPath) {
        super();
        LockPath = lockPath;
        String addr = PropertiesUtils.getProperties("zk.address");
        client = new ZkClient(addr);
        client.setZkSerializer(new MyZkSerializer());
        if (!this.client.exists(LockPath)) {
            try {
                this.client.createPersistent(LockPath);
            } catch (ZkNodeExistsException e) {

            }
        }
    }

    @Override
    public boolean tryLock() {
        if (this.currentPath == null) {
            currentPath = this.client.createEphemeralSequential(LockPath + "/", "lock");
        }
        // 获得所有的子
        List<String> children = this.client.getChildren(LockPath);

        // 排序list
        Collections.sort(children);

        // 判断当前节点是否是最小的
        if (!children.isEmpty()&&currentPath.equals(LockPath + "/" + children.get(0))) {
            return true;
        } else {
            // 取到前一个
            // 得到字节的索引号
            int curIndex = children.indexOf(currentPath.substring(LockPath.length() + 1));
            beforePath = LockPath + "/" + children.get(curIndex - 1);
        }
        return false;
    }

    @Override
    public void lock() {
        if (!tryLock()) {
            // 阻塞等待
            waitForLock();
            // 再次尝试加锁
            lock();
        }
    }

    private void waitForLock() {

        CountDownLatch cdl = new CountDownLatch(1);

        // 注册watcher
        IZkDataListener listener = new IZkDataListener() {

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println("-----监听到节点被删除");
                cdl.countDown();
            }

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {

            }
        };

        client.subscribeDataChanges(this.beforePath, listener);

        // 怎么让自己阻塞
        if (this.client.exists(this.beforePath)) {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // 醒来后，取消watcher
        client.unsubscribeDataChanges(this.beforePath, listener);
    }

    @Override
    public void unlock() {
        // 删除节点
        this.client.delete(this.currentPath);
        currentPath=null;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Condition newCondition() {
        // TODO Auto-generated method stub
        return null;
    }

}
