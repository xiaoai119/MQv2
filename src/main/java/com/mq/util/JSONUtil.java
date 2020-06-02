package com.mq.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.mq.common.QueueInfoOfTopic;
import com.mq.common.Topic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created By xfj on 2020/3/23
 */
public class JSONUtil {
    public static Topic JSON2Topic(JSONObject jsonObject) {
        Topic topic = new Topic("temp");
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            if (entry.getKey().equals("topicName")) {
                topic.setTopicName((String) entry.getValue());
            }
            if (entry.getKey().equals("queueNum")) {
                topic.setQueueNum((Integer) entry.getValue());
            }
            if (entry.getKey().equals("queues")) {
                JSONObject value = (JSONObject) entry.getValue();
                HashMap<String, List<Integer>> queues = JSON.parseObject(value.toJSONString(), new TypeReference<HashMap<String, List<Integer>>>() {
                });
                topic.setQueues(queues);
            }
        }
        return topic;
    }
}

