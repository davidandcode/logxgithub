package com.creditkarma.logx.utils.gcs;

import info.batey.kafka.unit.Zookeeper;


/**
 * Created by shengwei.wang on 11/24/16.
 */
public class ZooKeeperUnit {

        private Zookeeper zookeeper;



        public void startService(int port) {


        zookeeper =new Zookeeper(port);
        zookeeper.startup();
}


 public void shutDownService(){
         if (zookeeper != null) zookeeper.shutdown();
 }

}
