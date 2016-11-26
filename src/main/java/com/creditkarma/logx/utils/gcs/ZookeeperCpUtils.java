package com.creditkarma.logx.utils.gcs;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shengwei.wang on 11/19/16.
 *
 * Get zookeeper ensemble up and running
 *
 * Use the corrrect port number or just the hostname only
 */
public class ZookeeperCpUtils {


    public static void create(String path, long mydata,String hostport) throws
            KeeperException,InterruptedException {

        byte[] data = ByteUtils.longToBytes(mydata);
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);

            // try to create existing node will give you the exception
            // may need to change the CreateMode type to persistent + sequential for the same topic and partition
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        } catch (Exception e){

            // this is for testing purpose, need to use log4j
            System.out.println(e.toString());

        }finally {
            zk.close();
            conn.close();
        }
    }


    public static void delete(String path,String hostport) throws KeeperException,InterruptedException {

        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {


            // try to delete a node which doesn't exist will give you an exception
            zk = conn.connect(hostport);
            zk.delete(path,zk.exists(path,true).getVersion());

        } catch (Exception e){

            System.out.println(e.toString());
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



        } catch (Exception e){

            System.out.println("here: " + e.toString());

        }finally {




                zk.close();
                 conn.close();
        }

        resultFinal = ByteUtils.bytesToLong(result);
        return resultFinal;


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

/*
    public static void main(String[] args) throws Exception{
        //create("/ThursdayMorning",13579,"localhost");
        //Long myData = getData("/ThursdayMorning","localhost");
        //System.out.println(myData);

        update("/ThursdayMorning",1357,"localhost:2181");

        Long myData = getData("/ThursdayMorning","localhost:2181");
        System.out.println(myData);
    }
*/


}
