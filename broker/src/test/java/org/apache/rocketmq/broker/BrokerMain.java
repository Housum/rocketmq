package org.apache.rocketmq.broker;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * @author qibao
 * @since 2019-08-30
 */
public class BrokerMain {

    public static void main(String[] args) throws Exception{
        BrokerConfig brokerConfig = new BrokerConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        brokerConfig.setNamesrvAddr("localhost:9876");
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "41");
        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        brokerController.initialize();
        brokerController.start();

    }
}
