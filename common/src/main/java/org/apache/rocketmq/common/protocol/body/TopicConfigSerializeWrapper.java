/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.protocol.body;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * {
 * 	"dataVersion":{
 * 		"counter":6,
 * 		"timestamp":1558597619467
 *        },
 * 	"topicConfigTable":{
 * 		"TopicTest":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":4,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"TopicTest",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":4
 *        },
 * 		"TopicTestjjj":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":8,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"TopicTestjjj",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":8
 *        },
 * 		"SELF_TEST_TOPIC":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":1,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"SELF_TEST_TOPIC",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":1
 *        },
 * 		"DefaultCluster":{
 * 			"order":false,
 * 			"perm":7,
 * 			"readQueueNums":16,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"DefaultCluster",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":16
 *        },
 * 		"RMQ_SYS_TRANS_HALF_TOPIC":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":1,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"RMQ_SYS_TRANS_HALF_TOPIC",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":1
 *        },
 * 		"luqibaodeMacBook-Pro.local":{
 * 			"order":false,
 * 			"perm":7,
 * 			"readQueueNums":1,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"luqibaodeMacBook-Pro.local",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":1
 *        },
 * 		"TBW102":{
 * 			"order":false,
 * 			"perm":7,
 * 			"readQueueNums":8,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"TBW102",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":8
 *        },
 * 		"BenchmarkTest":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":1024,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"BenchmarkTest",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":1024
 *        },
 * 		"OFFSET_MOVED_EVENT":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":1,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"OFFSET_MOVED_EVENT",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":1
 *        },
 * 		"%RETRY%please_rename_unique_group_name_4":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":1,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"%RETRY%please_rename_unique_group_name_4",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":1
 *        },
 * 		"%RETRY%please_rename_unique_group_name_3":{
 * 			"order":false,
 * 			"perm":6,
 * 			"readQueueNums":1,
 * 			"topicFilterType":"SINGLE_TAG",
 * 			"topicName":"%RETRY%please_rename_unique_group_name_3",
 * 			"topicSysFlag":0,
 * 			"writeQueueNums":1
 *        }
 *    }
 * }
 *
 *
 * 保存topic信息以及topic的版本信息
 */
public class TopicConfigSerializeWrapper extends RemotingSerializable {
    //topicName - topic详细信息
    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
    //版本信息
    private DataVersion dataVersion = new DataVersion();

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}
