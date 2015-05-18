import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import static org.apache.zookeeper.AsyncCallback.DataCallback;
import static org.apache.zookeeper.AsyncCallback.StatCallback;
import static org.apache.zookeeper.AsyncCallback.StringCallback;
import static org.apache.zookeeper.AsyncCallback.VoidCallback;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.KeeperException.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Worker implements Watcher, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    public static String WORKERS_PATH_PREFIX = "/workers";

    ZooKeeper zk;
    String hostPort;
    String serverId;
    String status;
    String name;
    Executor executor;
    private volatile boolean connected;
    private volatile boolean expired;
    protected ChildrenCache assignedTasksCache;

    StringCallback createWorkerCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    LOGGER.info("Registered worker: " + serverId);
                    break;
                case NODEEXISTS:
                    LOGGER.warn("Already registered worker: " + serverId);
                    break;
                default:
                    LOGGER.error("Error creating worker: " + path + ".", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StatCallback statusUpdateCallback = new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
            }
        }
    };

    public Worker(String hostPort) {
        this.hostPort = hostPort;
        this.serverId = Long.toString(new Random().nextLong());
        this.name = "worker" + "-" + serverId;
        this.connected = false;
        this.expired = false;
        this.executor = new ThreadPoolExecutor(
                1,
                1,
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200)
        );
        this.assignedTasksCache = new ChildrenCache();
    }

    void bootstrap() {
        createAssignNode();
    }

    void createAssignNode() {
        zk.create("/assign/" + name, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT, createAssignCallback, null);
    }

    StringCallback createAssignCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignNode();
                    break;
                case OK:
                    LOGGER.info("Created assign node.");
                    break;
                case NODEEXISTS:
                    LOGGER.warn("Already created assign node.");
                    break;
                default:
                    LOGGER.error("Error creating assign node: " + path + ".", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void start() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing");
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOGGER.warn("ZooKeeper interrupted while closing");
        }
    }

    public void process(WatchedEvent e) {
        LOGGER.info("Process: " + e + ", " + hostPort);
        if (e.getType() == Event.EventType.None) {
            switch(e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOGGER.error("session expired");
                    break;
                default:
                    break;
            }
        }
    }

    void register() {
        zk.create(WORKERS_PATH_PREFIX + "/" + name,
            "Idle".getBytes(),
            OPEN_ACL_UNSAFE,
            EPHEMERAL,
            createWorkerCallback,
            null
        );
    }

    synchronized void updateStatus(String status) {
        if (this.status.equals(status)) {
            zk.setData(WORKERS_PATH_PREFIX + "/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    Watcher newTaskWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent e) {
            if (e.getType().equals(Event.EventType.NodeChildrenChanged)) {
                assert ("/assign/" + name).equals(e.getPath());
                getTasks();
            }
        }
    };

    void getTasks() {
        zk.getChildren("/assign/" + name, newTaskWatcher, tasksGetChildrenCallback, null);
    }

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (children != null) {
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback callback;

                            // initialize input of anonymous class
                            public Runnable init(List<String> children, DataCallback callback) {
                                this.children = children;
                                this.callback = callback;
                                return this;
                            }

                            @Override
                            public void run() {
                                if (children == null) {
                                    return;
                                }

                                LOGGER.info("Going through tasks");
                                setStatus("working");
                                for (String task : children) {
                                    LOGGER.trace("New task: {}", task);
                                    zk.getData("/assign/" + name + "/" + task, false, callback, task);
                                }
                            }
                        }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                    }
                    break;
                default:
                    LOGGER.error("Unable get child tasks.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    DataCallback taskDataCallback = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                    // execute a task by logging it
                    executor.execute(new Runnable() {
                        byte[] data;
                        Object ctx;

                        @Override
                        public void run() {
                            LOGGER.info("Executing task: " + new String(data));
                            zk.create("/status" + ctx, "done".getBytes(), OPEN_ACL_UNSAFE, PERSISTENT, taskStatusCreateCallback, null);
                            zk.delete("/assign/" + name + "/" + ctx, -1, taskVoidCallback, null);
                        }
                    });
                    break;
                default:
                    LOGGER.error("Unable get task data.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StringCallback taskStatusCreateCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.create(path + "/status", "done".getBytes(), OPEN_ACL_UNSAFE, PERSISTENT, taskStatusCreateCallback, null);
                    break;
                case OK:
                    LOGGER.info("Created status: " + name);
                    break;
                case NODEEXISTS:
                    LOGGER.warn("Status already exists: " + path);
                    break;
                default:
                    LOGGER.error("Unable create task status.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    VoidCallback taskVoidCallback = new VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    break;
                case OK:
                    LOGGER.info("Deleted task: " + path);
                    break;
                default:
                    LOGGER.error("Unable delete task.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker(args[0]);
        w.start();

        while (!w.isConnected()) {
            Thread.sleep(100);
        }

        w.bootstrap();
        w.register();
        w.getTasks();

        while (!w.isExpired()) {
            Thread.sleep(100000);
        }

    }

}
