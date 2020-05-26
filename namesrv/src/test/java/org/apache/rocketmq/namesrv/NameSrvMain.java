package org.apache.rocketmq.namesrv;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

import java.util.concurrent.TimeUnit;

/**
 * @author qibao
 * @since 2019-08-30
 */
public class NameSrvMain {

    public static void main(String[] args) throws Exception {
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);
        NamesrvController namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        namesrvController.initialize();
        namesrvController.start();
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }
}
