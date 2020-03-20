package mq.common.message;

import mq.common.IpNode;

import java.io.Serializable;

/**
 * Created By xfj on 2020/2/5
 */
public class PullMessage extends Message implements Serializable{
    private static final long serialVersionUID = -3037522746992937257L;
    private static final int type = MessageType.PULL;
    public PullMessage(IpNode ipNode, String message, int num) {
        this.num = num;
        this.node = ipNode;
        this.message = message;
    }
    public IpNode getIpNode() {
        return node;
    }
    public int getNum() {
        return num;
    }
    public int getType() {
        return type;
    }
    public String getMessage() {
        return message;
    }
}
