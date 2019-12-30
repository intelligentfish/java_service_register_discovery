package com.neo.wang.zktest;

import lombok.SneakyThrows;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 服务注册发现
 */
public class ServiceRegisterDiscovery implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegisterDiscovery.class);// 日志
    private volatile boolean connectedFlag;// 连接标志
    private String zkAddress;// ZK地址
    private int zkTimeout;// 超时
    private ZooKeeper zk;// ZK客户端
    private CountDownLatch connectCountDownLatch;// 计数器
    private Map<String, Map<String, byte[]>> discoveryDataMap;// 发现数据
    private Map<String, List<Function<List<String>, Void>>> discoveryCallback;// 发现回调

    /**
     * 目录迭代
     *
     * @param path     全路径 eg. /ip/127.0.0.1:1001
     * @param callback 路径回调 /ip /ip/127.0.0.0:1001
     * @return 异常
     */
    private Exception pathIterator(String path, Function<String, Exception> callback) {
        Exception ex = null;
        int end = path.indexOf("/", '/' == path.charAt(0) ? 1 : 0);
        for (; -1 != end; ) {
            ex = callback.apply(path.substring(0, end));
            if (null != ex) return ex;
            if ('/' == path.charAt(end)) end++;
            end = path.indexOf("/", end);
        }
        ex = callback.apply(path);
        return ex;
    }

    /**
     * 连接Zookeeper服务
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void connect() throws IOException, InterruptedException {
        this.connectCountDownLatch = new CountDownLatch(1);
        this.zk = new ZooKeeper(this.zkAddress, this.zkTimeout, this);
        this.connectCountDownLatch.await();
    }

    /**
     * 获取孩子并监视
     *
     * @param path 监视目录 eg. /ip
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void getChildrenAndWatch(String path) throws KeeperException, InterruptedException {
        // 获取最新的孩子列表
        List<String> children = this.zk.getChildren(path, this);
        // 孩子全路径集合
        Set<String> notifyChildrenFullPathSet = new HashSet<>();
        // 复制所有回调
        List<Function<List<String>, Void>> changedCallbackList = new LinkedList<>();
        synchronized (this.discoveryCallback) {
            List<Function<List<String>, Void>> functions = this.discoveryCallback.get(path);
            if (null != functions && !functions.isEmpty())
                for (Function<List<String>, Void> function : functions) changedCallbackList.add(function);
        }

        do {
            // 结点全部下线
            if (null == children || children.isEmpty()) {
                synchronized (this.discoveryDataMap) {
                    Map<String, byte[]> stringMap = this.discoveryDataMap.get(path);
                    if (null == stringMap) break; // 之前和现在都没有孩子结点

                    // 获取之前的孩子结点全路径
                    Set<String> strings = stringMap.keySet();
                    if (null == strings || strings.isEmpty()) break;

                    // 复制全路径
                    for (String fullPath : strings) notifyChildrenFullPathSet.add(fullPath);
                    this.discoveryDataMap.clear();// 清空
                }
                break;
            }

            // 新孩子全路径列表集合
            Set<String> childrenNewFullPathSet = new HashSet<>();
            for (String child : children) {
                String fullPath = String.format("%s/%s", path, child);
                childrenNewFullPathSet.add(fullPath);// 添加到新的孩子全路径列表
                notifyChildrenFullPathSet.add(fullPath); // 添加到通知改变孩子全路径列表
            }

            synchronized (this.discoveryDataMap) {
                Map<String, byte[]> stringMap = this.discoveryDataMap.get(path);
                if (null != stringMap) {
                    Set<String> childrenOldFullPathSet = stringMap.keySet();
                    // 老的集合有，新的集合没有结点，删除
                    for (String fullPath : childrenOldFullPathSet) {
                        if (childrenNewFullPathSet.contains(fullPath)) continue;

                        notifyChildrenFullPathSet.add(fullPath);
                        stringMap.remove(fullPath);
                    }
                }

                // 获取最新数据
                if (null == stringMap) stringMap = new HashMap<>();
                for (String child : children) {
                    String fullPath = String.format("%s/%s", path, child);
                    byte[] data = this.zk.getData(fullPath, null, null);
                    stringMap.put(fullPath, data);
                }

                // 写入新的数据
                this.discoveryDataMap.put(path, stringMap);
            }
        } while (false);

        // 通知路径改变
        for (Function<List<String>, Void> function : changedCallbackList) {
            function.apply(notifyChildrenFullPathSet.stream().collect(Collectors.toList()));
        }
    }

    /**
     * 处理ZK事件
     *
     * @param event ZK事件
     */
    @Override
    public void process(WatchedEvent event) {
        LOGGER.info("zk event type: {}, state: {}", event.getType(), event.getState());
        // 处理首次连接
        if (event.getType() == Watcher.Event.EventType.None) {
            this.connectedFlag = event.getState() == Watcher.Event.KeeperState.SyncConnected;
            this.connectCountDownLatch.countDown();
            return;
        }

        // 处理具体事件
        switch (event.getState()) {
            case SyncConnected: {
                // 连接
                switch (event.getType()) {
                    case NodeChildrenChanged: {
                        try {
                            this.getChildrenAndWatch(event.getPath());
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                }
            }
            break;

            case Disconnected: {
                // 连接断掉
            }
            break;

            case Expired: {
                // 处理会话过期
                try {
                    this.connect();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            break;
        }
    }

    /**
     * 构造方法
     *
     * @param address 地址 eg. xxx.com:2181,wsl.yyy:2182,zzz:wsl.com:2183
     * @param timeout
     */
    @SneakyThrows
    public ServiceRegisterDiscovery(String address, int timeout) {
        this.zkAddress = address;
        this.zkTimeout = timeout;
        this.discoveryDataMap = new HashMap<>();
        this.discoveryCallback = new HashMap<>();
        this.connect();
    }

    /**
     * 获取连接标志
     *
     * @return 连接标志
     */
    public boolean getConnectFlag() {
        return this.connectedFlag;
    }

    /**
     * 获取服务数据
     *
     * @param fullPath 全路径
     * @return 服务配置值
     */
    public Map<String, byte[]> getServiceData(String fullPath) {
        Map<String, byte[]> clone = new HashMap<>();
        synchronized (this.discoveryDataMap) {
            Map<String, byte[]> stringMap = this.discoveryDataMap.get(fullPath);
            if (null != stringMap) stringMap.forEach((s, bytes) -> clone.put(s, bytes));
        }
        return clone;
    }

    /**
     * 服务注册
     *
     * @param fullPath 全路径 eg. /ip/127.0.0.1:10001
     * @param data     注册数据
     * @return
     */
    @SneakyThrows
    public ServiceRegisterDiscovery serviceRegister(String fullPath, byte[] data) {
        Exception e = this.pathIterator(fullPath, current -> {
            try {
                if (null != this.zk.exists(current, false)) return null;
                boolean equals = current.equals(fullPath);
                this.zk.create(current,
                        equals ? data : null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        equals ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT);
            } catch (Exception ex) {
                return ex;
            }
            return null;
        });
        if (null != e) throw e;
        return this;
    }

    /**
     * 服务发现
     *
     * @param topPath      顶级路径 eg. /ip
     * @param onPathChange 改变回调
     * @return
     */
    @SneakyThrows
    public ServiceRegisterDiscovery serviceDiscovery(String topPath,
                                                     Function<List<String>, Void> onPathChange) {
        do {
            if (null == this.zk.exists(topPath, false)) break;
            synchronized (this.discoveryCallback) {
                List<Function<List<String>, Void>> functions = this.discoveryCallback.get(topPath);
                if (null == functions) functions = new LinkedList<>();
                functions.add(onPathChange);
                this.discoveryCallback.put(topPath, functions);
            }
            this.getChildrenAndWatch(topPath);
        } while (false);
        return this;
    }
}
