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
package org.apache.rocketmq.client.producer;

import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息发送的选择器 通过该选择器 可以计算出最终消息发送的MessageQueue
 * 对于消息发送到同一个MessageQueue来说,consumer消费的时候能够保证有序的消息
 * 底层实现就是创建一个consumeQueue
 *
 */
public interface MessageQueueSelector {
    /**
     * @param mqs 所有的MessageQueue
     * @param msg 消息
     * @param arg 应用传入的参数 透传
     */
    MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
