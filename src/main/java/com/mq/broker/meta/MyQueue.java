package com.mq.broker.meta;

import com.mq.common.message.Message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created By xfj on 2020/3/15
 */
public class MyQueue implements Serializable {
    private ArrayBlockingQueue<Message> queue;

    public MyQueue() {
        queue = new ArrayBlockingQueue<Message>(10000);
    }
    public void putAtHeader(Message value) {
        queue.add(value);
    }



    public void addAll(List<Message> messages){
        queue.addAll(messages);
    }

    public void remove(Message message){
        queue.remove(message);
    }

    public int size() {
        return queue.size();
    }

    public void getAll() {
        Iterator<Message> iterator = queue.iterator();
        while(iterator.hasNext()){
            System.out.print(iterator.next().getMessage()+" ");
        }
        System.out.println();
    }

    public List<Message> drainTo(int size){
        List<Message> messages = new ArrayList<>(size);
        queue.drainTo(messages,size);
        return messages;
    }

    //逆序输出
    public List<Message> getReverseAll() {
        Iterator<Message> iterator = queue.iterator();
        LinkedList<Message> list = new LinkedList<Message>();
        while(iterator.hasNext()){
            list.addFirst(iterator.next());
        }
        return list;
    }
}