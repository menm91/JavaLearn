package me.qianlv.zookeeper;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZookeeperExample {
    private ZooKeeper zooKeeper = null;
    private final String connectString = "192.168.27.141:2181,192.168.27.142:2181,192.168.27.143:2181";
    private static final int TIMEOUT = 2000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Before
    public void initZookeeper() throws IOException, InterruptedException {
        System.out.println("初始化 Zookeeper...");
        zooKeeper = new ZooKeeper(connectString, TIMEOUT, (event) -> {
            //事件监听回调函数
            if (countDownLatch.getCount() > 0 && event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                countDownLatch.countDown();
            }

            System.out.println(event.getType());
            System.out.println(event.getPath());
        });

        countDownLatch.await();
        System.out.println("初始化完成...");
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        //参数1: 路径 参数2: 保存的数据 参数3: 权限 参数4: 节点类型
        zooKeeper.create("/hello", "Hello Zookeeper".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void update() throws KeeperException, InterruptedException {
        zooKeeper.setData("/hello", "update Zookeeper".getBytes(), 0);
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        zooKeeper.delete("/hello", 1);
    }

    @Test
    public void query() throws KeeperException, InterruptedException {
        //参数0: 路径 参数2: 是否使用之前(客户端)的事件函数 参数3: 数据的元信息
        byte[] data = zooKeeper.getData("/hello", true, null);
        System.out.println(data);
    }

    @After
    public void after() throws InterruptedException {
        System.out.println("执行完成了.");
        zooKeeper.close();
    }
}
