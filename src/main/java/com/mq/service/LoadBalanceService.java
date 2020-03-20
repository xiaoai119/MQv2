package com.mq.service;

import java.util.HashMap;
import java.util.List;

/**
 * Created By xfj on 2020/3/16
 */
public interface LoadBalanceService {
    HashMap<String,String> loadBlanceByTpoicName(List<String> topicNames);
}
