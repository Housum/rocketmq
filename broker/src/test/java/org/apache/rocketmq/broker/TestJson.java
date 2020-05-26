package org.apache.rocketmq.broker;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.junit.Test;

/**
 * @author qibao
 * @since 2019-08-30
 */
public class TestJson {

    @Test
    public void testJSON(){

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = JSON.parseObject("{\"filterServerList\":[],\"topicConfigSerializeWrapper\":{\"dataVersion\":{\"counter\":6,\"timestamp\":1558597619467},\"topicConfigTable\":{\"TopicTest\":{\"order\":false,\"perm\":6,\"readQueueNums\":4,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"TopicTest\",\"topicSysFlag\":0,\"writeQueueNums\":4},\"TopicTestjjj\":{\"order\":false,\"perm\":6,\"readQueueNums\":8,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"TopicTestjjj\",\"topicSysFlag\":0,\"writeQueueNums\":8},\"SELF_TEST_TOPIC\":{\"order\":false,\"perm\":6,\"readQueueNums\":1,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"SELF_TEST_TOPIC\",\"topicSysFlag\":0,\"writeQueueNums\":1},\"DefaultCluster\":{\"order\":false,\"perm\":7,\"readQueueNums\":16,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"DefaultCluster\",\"topicSysFlag\":0,\"writeQueueNums\":16},\"RMQ_SYS_TRANS_HALF_TOPIC\":{\"order\":false,\"perm\":6,\"readQueueNums\":1,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"RMQ_SYS_TRANS_HALF_TOPIC\",\"topicSysFlag\":0,\"writeQueueNums\":1},\"luqibaodeMacBook-Pro.local\":{\"order\":false,\"perm\":7,\"readQueueNums\":1,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"luqibaodeMacBook-Pro.local\",\"topicSysFlag\":0,\"writeQueueNums\":1},\"TBW102\":{\"order\":false,\"perm\":7,\"readQueueNums\":8,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"TBW102\",\"topicSysFlag\":0,\"writeQueueNums\":8},\"BenchmarkTest\":{\"order\":false,\"perm\":6,\"readQueueNums\":1024,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"BenchmarkTest\",\"topicSysFlag\":0,\"writeQueueNums\":1024},\"OFFSET_MOVED_EVENT\":{\"order\":false,\"perm\":6,\"readQueueNums\":1,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"OFFSET_MOVED_EVENT\",\"topicSysFlag\":0,\"writeQueueNums\":1},\"%RETRY%please_rename_unique_group_name_4\":{\"order\":false,\"perm\":6,\"readQueueNums\":1,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"%RETRY%please_rename_unique_group_name_4\",\"topicSysFlag\":0,\"writeQueueNums\":1},\"%RETRY%please_rename_unique_group_name_3\":{\"order\":false,\"perm\":6,\"readQueueNums\":1,\"topicFilterType\":\"SINGLE_TAG\",\"topicName\":\"%RETRY%please_rename_unique_group_name_3\",\"topicSysFlag\":0,\"writeQueueNums\":1}}}}",TopicConfigSerializeWrapper.class);
        System.out.println(topicConfigSerializeWrapper.getTopicConfigTable());
    }
}
