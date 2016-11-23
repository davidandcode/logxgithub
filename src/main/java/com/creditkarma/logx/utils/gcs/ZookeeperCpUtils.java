package com.creditkarma.logx.utils.gcs;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shengwei.wang on 11/19/16.
 */
public class ZookeeperCpUtils {


    // shut down zk first and conn then
    // (start a conn and then start zk)


    public static void create(String path, long mydata,String hostport) throws
            KeeperException,InterruptedException {

        byte[] data = ByteUtils.longToBytes(mydata);
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        } catch (Exception e){

        }finally {
            zk.close();
            conn.close();
        }
    }


    public static void delete(String path,String hostport) throws KeeperException,InterruptedException {

        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            zk.delete(path,zk.exists(path,true).getVersion());

        } catch (Exception e){

        }
        finally {

            zk.close();
            conn.close();
        }
    }


    public static boolean znodeExists(String path,String hostport) throws
            KeeperException,InterruptedException {

        boolean result = false;
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            result= zk.exists(path, true) == null?false:true;

        } catch (Exception e){

        }
finally {
            zk.close();
            conn.close();


        }
        return result;
    }

    public static List<String> getChildren(String path,String hostport) throws
            KeeperException,InterruptedException {

        List<String> result = new ArrayList<String>();

        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            if(znodeExists(path,hostport))
                result = zk.getChildren(path,false);

        } catch (Exception e){

        }finally {


            zk.close();
            conn.close();
        }
        return result;

    }


    public static long getData(String path,String hostport) throws Exception {
        byte[] result = null;
        long resultFinal = -1;
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            if(znodeExists(path,hostport))
                result = zk.getData(path,false,null);

            resultFinal = ByteUtils.bytesToLong(result);

        } catch (Exception e){

        }finally {


            zk.close();
            conn.close();
        }return resultFinal;


    }

    public static void update(String path, long mydata,String hostport) throws
            KeeperException,InterruptedException {

        byte[] data = ByteUtils.longToBytes(mydata);
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            zk.setData(path, data, zk.exists(path,true).getVersion());

        } catch (Exception e){

        }finally {


            zk.close();
            conn.close();
        }

    }


}
