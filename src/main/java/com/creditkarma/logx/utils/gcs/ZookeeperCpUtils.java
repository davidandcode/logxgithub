package com.creditkarma.logx.utils.gcs;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shengwei.wang on 11/19/16.
 */
public class ZookeeperCpUtils {

    private static ZooKeeperConnection conn = new ZooKeeperConnection();

    private static ZooKeeper zk = null;


    public static ZooKeeper getAZookeeper(String hostport) {



        ZooKeeper result = null;

        if(zk == null) {

            try {
                result = conn.connect(hostport);
                zk = result;
            } catch (Exception e) {

            }
        }else{
            result = zk;
        }


        return result;

    }

    public static void close(ZooKeeperConnection conn) throws InterruptedException {
        conn.close();
    }




    public static void shutZooKeeper(ZooKeeper zk){
        try {
            zk.close();
        } catch (Exception e){

        }
    }


    public static void create(String path, byte[] data, ZooKeeper zk) throws
            KeeperException,InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }


    public static void delete(String path,ZooKeeper zk) throws KeeperException,InterruptedException {
        zk.delete(path,zk.exists(path,true).getVersion());
    }


    public static boolean znodeExists(String path,ZooKeeper zk) throws
            KeeperException,InterruptedException {
        return zk.exists(path, true) == null?false:true;
    }

    public static List<String> getChildren(String path,ZooKeeper zk) throws
            KeeperException,InterruptedException {

        List<String> result = new ArrayList<String>();

        if(znodeExists(path,zk))
            return zk.getChildren(path,false);

        return result;
    }


    public static byte[] getData(String path,ZooKeeper zk) throws Exception {
        byte[] result = null;

        if(znodeExists(path,zk))
            return zk.getData(path,false,null);


        return result;

    }

    public static void update(String path, byte[] data,ZooKeeper zk) throws
            KeeperException,InterruptedException {
        zk.setData(path, data, zk.exists(path,true).getVersion());
    }


}
