package mq.broker;

import mq.common.message.Message;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created By xfj on 2020/3/15
 */
public class MyQueue implements Serializable {

    private static final long serialVersionUID = 1L;
    volatile static int count = 1;//测试
    private ConcurrentLinkedDeque<Message> queue;

    public MyQueue() {
        queue = new ConcurrentLinkedDeque<Message>();
    }
    public void putAtHeader(Message value) {
        queue.addFirst(value);
    }
    public Message getAndRemoveTail() {
        return queue.pollLast();
    }
    public Message getTail() {
        return queue.peekLast();
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