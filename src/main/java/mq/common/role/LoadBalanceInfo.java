package mq.common.role;

import java.util.List;

/**
 * Created By xfj on 2020/3/17
 */
public class LoadBalanceInfo extends RoleInfo {
    public LoadBalanceInfo() {
        this.setName("loadbalance");
    }

    public LoadBalanceInfo(String ip, int port, String name) {
        this.ip = ip;
        this.port = port;
        this.name = name;
        this.role="loadbalance";
    }
}
