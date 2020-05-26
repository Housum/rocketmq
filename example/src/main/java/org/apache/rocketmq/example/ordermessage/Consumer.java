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
package org.apache.rocketmq.example.ordermessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class Consumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TopicTest", "TagA || TagC || TagD");

        /**
         * 同一个ID 是有序的,但是可能出现发现前一个依赖消息未来的情况，可以返回SUSPEND_CURRENT_QUEUE_A_MOMENT
         * 那么该条消息就重新被投入到队列中 等待前置消息完成之后再次收到
         * ConsumeMessageThread_6 Receive New Messages: Hello RocketMQ orderId = 0,i = 0
         * ConsumeMessageThread_7 Receive New Messages: Hello RocketMQ orderId = 1,i = 1
         * ConsumeMessageThread_8 Receive New Messages: Hello RocketMQ orderId = 2,i = 2
         * ConsumeMessageThread_9 Receive New Messages: Hello RocketMQ orderId = 3,i = 3
         * ConsumeMessageThread_10 Receive New Messages: Hello RocketMQ orderId = 4,i = 4
         * ConsumeMessageThread_11 Receive New Messages: Hello RocketMQ orderId = 5,i = 5
         * ConsumeMessageThread_12 Receive New Messages: Hello RocketMQ orderId = 6,i = 6
         * ConsumeMessageThread_13 Receive New Messages: Hello RocketMQ orderId = 7,i = 7
         * ConsumeMessageThread_14 Receive New Messages: Hello RocketMQ orderId = 8,i = 8
         * ConsumeMessageThread_15 Receive New Messages: Hello RocketMQ orderId = 9,i = 9
         * ConsumeMessageThread_16 Receive New Messages: Hello RocketMQ orderId = 0,i = 10
         * ConsumeMessageThread_17 Receive New Messages: Hello RocketMQ orderId = 1,i = 11
         * ConsumeMessageThread_18 Receive New Messages: Hello RocketMQ orderId = 2,i = 12
         * ConsumeMessageThread_19 Receive New Messages: Hello RocketMQ orderId = 3,i = 13
         * ConsumeMessageThread_20 Receive New Messages: Hello RocketMQ orderId = 4,i = 14
         * ConsumeMessageThread_2 Receive New Messages: Hello RocketMQ orderId = 5,i = 15
         * ConsumeMessageThread_1 Receive New Messages: Hello RocketMQ orderId = 6,i = 16
         * ConsumeMessageThread_4 Receive New Messages: Hello RocketMQ orderId = 7,i = 17
         * ConsumeMessageThread_5 Receive New Messages: Hello RocketMQ orderId = 8,i = 18
         * ConsumeMessageThread_3 Receive New Messages: Hello RocketMQ orderId = 9,i = 19
         * ConsumeMessageThread_6 Receive New Messages: Hello RocketMQ orderId = 0,i = 20
         * ConsumeMessageThread_7 Receive New Messages: Hello RocketMQ orderId = 1,i = 21
         * ConsumeMessageThread_8 Receive New Messages: Hello RocketMQ orderId = 2,i = 22
         * ConsumeMessageThread_9 Receive New Messages: Hello RocketMQ orderId = 3,i = 23
         * ConsumeMessageThread_10 Receive New Messages: Hello RocketMQ orderId = 4,i = 24
         * ConsumeMessageThread_11 Receive New Messages: Hello RocketMQ orderId = 5,i = 25
         * ConsumeMessageThread_12 Receive New Messages: Hello RocketMQ orderId = 6,i = 26
         * ConsumeMessageThread_13 Receive New Messages: Hello RocketMQ orderId = 7,i = 27
         * ConsumeMessageThread_14 Receive New Messages: Hello RocketMQ orderId = 8,i = 28
         * ConsumeMessageThread_15 Receive New Messages: Hello RocketMQ orderId = 9,i = 29
         * ConsumeMessageThread_16 Receive New Messages: Hello RocketMQ orderId = 0,i = 30
         * ConsumeMessageThread_17 Receive New Messages: Hello RocketMQ orderId = 1,i = 31
         * ConsumeMessageThread_18 Receive New Messages: Hello RocketMQ orderId = 2,i = 32
         * ConsumeMessageThread_19 Receive New Messages: Hello RocketMQ orderId = 3,i = 33
         * ConsumeMessageThread_20 Receive New Messages: Hello RocketMQ orderId = 4,i = 34
         * ConsumeMessageThread_2 Receive New Messages: Hello RocketMQ orderId = 5,i = 35
         * ConsumeMessageThread_1 Receive New Messages: Hello RocketMQ orderId = 6,i = 36
         * ConsumeMessageThread_4 Receive New Messages: Hello RocketMQ orderId = 7,i = 37
         * ConsumeMessageThread_5 Receive New Messages: Hello RocketMQ orderId = 8,i = 38
         * ConsumeMessageThread_3 Receive New Messages: Hello RocketMQ orderId = 9,i = 39
         * ConsumeMessageThread_6 Receive New Messages: Hello RocketMQ orderId = 0,i = 40
         * ConsumeMessageThread_7 Receive New Messages: Hello RocketMQ orderId = 1,i = 41
         * ConsumeMessageThread_8 Receive New Messages: Hello RocketMQ orderId = 2,i = 42
         * ConsumeMessageThread_9 Receive New Messages: Hello RocketMQ orderId = 3,i = 43
         * ConsumeMessageThread_10 Receive New Messages: Hello RocketMQ orderId = 4,i = 44
         * ConsumeMessageThread_11 Receive New Messages: Hello RocketMQ orderId = 5,i = 45
         * ConsumeMessageThread_12 Receive New Messages: Hello RocketMQ orderId = 6,i = 46
         * ConsumeMessageThread_13 Receive New Messages: Hello RocketMQ orderId = 7,i = 47
         * ConsumeMessageThread_14 Receive New Messages: Hello RocketMQ orderId = 8,i = 48
         * ConsumeMessageThread_15 Receive New Messages: Hello RocketMQ orderId = 9,i = 49
         * ConsumeMessageThread_16 Receive New Messages: Hello RocketMQ orderId = 0,i = 50
         * ConsumeMessageThread_17 Receive New Messages: Hello RocketMQ orderId = 1,i = 51
         * ConsumeMessageThread_18 Receive New Messages: Hello RocketMQ orderId = 2,i = 52
         * ConsumeMessageThread_19 Receive New Messages: Hello RocketMQ orderId = 3,i = 53
         * ConsumeMessageThread_20 Receive New Messages: Hello RocketMQ orderId = 4,i = 54
         * ConsumeMessageThread_2 Receive New Messages: Hello RocketMQ orderId = 5,i = 55
         * ConsumeMessageThread_1 Receive New Messages: Hello RocketMQ orderId = 6,i = 56
         * ConsumeMessageThread_4 Receive New Messages: Hello RocketMQ orderId = 7,i = 57
         * ConsumeMessageThread_5 Receive New Messages: Hello RocketMQ orderId = 8,i = 58
         * ConsumeMessageThread_3 Receive New Messages: Hello RocketMQ orderId = 9,i = 59
         * ConsumeMessageThread_6 Receive New Messages: Hello RocketMQ orderId = 0,i = 60
         * ConsumeMessageThread_7 Receive New Messages: Hello RocketMQ orderId = 1,i = 61
         * ConsumeMessageThread_8 Receive New Messages: Hello RocketMQ orderId = 2,i = 62
         * ConsumeMessageThread_9 Receive New Messages: Hello RocketMQ orderId = 3,i = 63
         * ConsumeMessageThread_10 Receive New Messages: Hello RocketMQ orderId = 4,i = 64
         * ConsumeMessageThread_11 Receive New Messages: Hello RocketMQ orderId = 5,i = 65
         * ConsumeMessageThread_12 Receive New Messages: Hello RocketMQ orderId = 6,i = 66
         * ConsumeMessageThread_13 Receive New Messages: Hello RocketMQ orderId = 7,i = 67
         * ConsumeMessageThread_14 Receive New Messages: Hello RocketMQ orderId = 8,i = 68
         * ConsumeMessageThread_15 Receive New Messages: Hello RocketMQ orderId = 9,i = 69
         * ConsumeMessageThread_16 Receive New Messages: Hello RocketMQ orderId = 0,i = 70
         * ConsumeMessageThread_17 Receive New Messages: Hello RocketMQ orderId = 1,i = 71
         * ConsumeMessageThread_18 Receive New Messages: Hello RocketMQ orderId = 2,i = 72
         * ConsumeMessageThread_19 Receive New Messages: Hello RocketMQ orderId = 3,i = 73
         * ConsumeMessageThread_20 Receive New Messages: Hello RocketMQ orderId = 4,i = 74
         * ConsumeMessageThread_2 Receive New Messages: Hello RocketMQ orderId = 5,i = 75
         * ConsumeMessageThread_1 Receive New Messages: Hello RocketMQ orderId = 6,i = 76
         * ConsumeMessageThread_4 Receive New Messages: Hello RocketMQ orderId = 7,i = 77
         * ConsumeMessageThread_5 Receive New Messages: Hello RocketMQ orderId = 8,i = 78
         * ConsumeMessageThread_3 Receive New Messages: Hello RocketMQ orderId = 9,i = 79
         * ConsumeMessageThread_6 Receive New Messages: Hello RocketMQ orderId = 0,i = 80
         * ConsumeMessageThread_7 Receive New Messages: Hello RocketMQ orderId = 1,i = 81
         * ConsumeMessageThread_8 Receive New Messages: Hello RocketMQ orderId = 2,i = 82
         * ConsumeMessageThread_9 Receive New Messages: Hello RocketMQ orderId = 3,i = 83
         * ConsumeMessageThread_10 Receive New Messages: Hello RocketMQ orderId = 4,i = 84
         * ConsumeMessageThread_11 Receive New Messages: Hello RocketMQ orderId = 5,i = 85
         * ConsumeMessageThread_12 Receive New Messages: Hello RocketMQ orderId = 6,i = 86
         * ConsumeMessageThread_13 Receive New Messages: Hello RocketMQ orderId = 7,i = 87
         * ConsumeMessageThread_14 Receive New Messages: Hello RocketMQ orderId = 8,i = 88
         * ConsumeMessageThread_15 Receive New Messages: Hello RocketMQ orderId = 9,i = 89
         * ConsumeMessageThread_16 Receive New Messages: Hello RocketMQ orderId = 0,i = 90
         * ConsumeMessageThread_17 Receive New Messages: Hello RocketMQ orderId = 1,i = 91
         * ConsumeMessageThread_18 Receive New Messages: Hello RocketMQ orderId = 2,i = 92
         * ConsumeMessageThread_19 Receive New Messages: Hello RocketMQ orderId = 3,i = 93
         * ConsumeMessageThread_20 Receive New Messages: Hello RocketMQ orderId = 4,i = 94
         * ConsumeMessageThread_2 Receive New Messages: Hello RocketMQ orderId = 5,i = 95
         * ConsumeMessageThread_1 Receive New Messages: Hello RocketMQ orderId = 6,i = 96
         * ConsumeMessageThread_4 Receive New Messages: Hello RocketMQ orderId = 7,i = 97
         * ConsumeMessageThread_5 Receive New Messages: Hello RocketMQ orderId = 8,i = 98
         * ConsumeMessageThread_3 Receive New Messages: Hello RocketMQ orderId = 9,i = 99
         */
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(false);
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
