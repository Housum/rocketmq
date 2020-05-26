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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 远程通信 服务端实现
 */
public interface RemotingServer extends RemotingService {

    /**
     * 注册请求码对应的请求处理器
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);
    /**
     * 注册请求处理器
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);
    /**
     * 本地的监听端口
     */
    int localListenPort();
    /**
     * 针对于请求码 获取到处理器以及对于的线程池
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 同步的调用 返回结果
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException;

    /**
     * 异步的调用
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 没有返回结果
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException;

}
