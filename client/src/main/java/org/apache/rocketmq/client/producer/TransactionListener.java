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

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务消息的确定 因为rocket是通过2pc+回调实现的所以需要提供执行方法以及查询状态的方法
 */
public interface TransactionListener {
    /**
     * 在发送事物消息到broker之后 是将企鹅放入到了
     * half这个topic中,之后再进行回调这个方法执行本地的事务
     * 本地执行完成事务之后，需要将事务的执行状态返回给broker,
     * broker根据执行的状态进行处理
     * 1.如果成功的话 那么将事务消息放入到真实的topic中
     * 2.如果状态未知的情况下 那么会定时的调用checkLocalTransaction方法校验事务的状态
     * 3.如果是失败的话 那么broker直接从half中清理掉事务消息
     *
     * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
     *
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return Transaction state
     */
    LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

    /**
     * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
     * method will be invoked to get local transaction status.
     *
     * @param msg Check message
     * @return Transaction state
     */
    LocalTransactionState checkLocalTransaction(final MessageExt msg);
}