package mq.service;

import mq.loadbalance.LoadBalance;

import java.util.HashMap;
import java.util.List;

/**
 * Created By xfj on 2020/3/16
 */
public class LoadBalanceServiceImpl implements LoadBalanceService {
    LoadBalance loadBlance;
    public LoadBalanceServiceImpl(LoadBalance loadBlance) {
        this.loadBlance = loadBlance;
    }

    /**
     * @param topicNames
     * @return 返回topicNames，BrokerName的哈希表
     */
    @Override
    public HashMap<String, String> loadBlanceByTpoicName(List<String> topicNames) {
        return loadBlance.loadBlanceByTpoicNames(topicNames);
    }
}
