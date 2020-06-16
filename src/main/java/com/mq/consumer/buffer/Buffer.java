package com.mq.consumer.buffer;

import com.mq.common.message.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created By xfj on 2020/6/3
 * dynamic push/pull 用到的buffer，缓存获取到的消息
 */
public class Buffer {
    private ArrayBlockingQueue<Message> buffer;
    int capacity;
    //预分配容量，这里先不考虑上下溢的问题
    AtomicInteger preAllocateSize;
    //设置阈值。若容量超过阈值则暂时不可继续发起pushRequest
    double requestThreshold;


    public Buffer(int capacity,double requestThreshold) {
        buffer = new ArrayBlockingQueue<Message>(capacity);
        this.capacity=capacity;
        preAllocateSize=new AtomicInteger(0);
        this.requestThreshold=requestThreshold;
    }

    public int getRemainCapacity(){
        return capacity-buffer.size()-preAllocateSize.get();
    }

    //预分配buffer容量
    public boolean preAllocate(int size){
        if(getRemainCapacity()>size){
            preAllocateSize.getAndAdd(size);
            return true;
        }
        return false;
    }

    /**
     * 释放预分配的buffer
     * @param releseSize
     * @return
     */
    public synchronized boolean relese(int releseSize){
        if(preAllocateSize.get()>releseSize&&releseSize>0){
            System.out.println("释放buffet大小"+releseSize);
            preAllocateSize.getAndAdd(-releseSize);
            return true;
        }
        return false;
    }

    //根据当前容量与阈值判断是否可以向broker发起push请求
    public boolean canRequest(){
        if((double)getRemainCapacity()/capacity>(1-requestThreshold))
            return true;
        return false;
    }

    public void addToBuffer(List<Message> messages){
        buffer.addAll(messages);
    }

    public List<Message> getMessageFromBuffer(int size){
        List<Message> messages = new ArrayList<>(size);
        buffer.drainTo(messages,size);
        return messages;
    }
}
