import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.AsyncCallback.DataCallback;
import static org.apache.zookeeper.AsyncCallback.StatCallback;
import static org.apache.zookeeper.AsyncCallback.VoidCallback;
import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;
import static org.apache.zookeeper.KeeperException.Code;
import static org.apache.zookeeper.KeeperException.ConnectionLossException;
import static org.apache.zookeeper.KeeperException.NodeExistsException;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Client implements Watcher, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    ZooKeeper zk;
    String hostPort;
    protected ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();
    private volatile boolean connected;
    private volatile boolean expired;

    public Client(String hostPort) {
        this.hostPort = hostPort;
        this.connected = false;
        this.expired = false;
    }

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

    String queueCommand(String command) throws KeeperException, Exception {
        while (true) {
            try {
                String name = zk.create("/tasks/task-", command.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT_SEQUENTIAL);
                return name;
            } catch (NodeExistsException e) {
                throw new Exception(command + " already running");
            } catch (ConnectionLossException e) {
                // keep trying to queue command
            }
        }
    }

    void submitTask(String task, TaskObject taskCtx) {
        taskCtx.setTask(task);
        zk.create(
                "/tasks/task-",
                task.getBytes(),
                OPEN_ACL_UNSAFE,
                PERSISTENT_SEQUENTIAL,
                createTaskCallback,
                taskCtx
        );
    }

    AsyncCallback.StringCallback createTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    // this might create duplicate tasks
                    submitTask(
                            ((TaskObject) ctx).getTask(),
                            (TaskObject) ctx
                    );
                    break;
                case OK:
                    LOGGER.info("Created task: " + name);
                    ((TaskObject) ctx).setTaskName(name);
                    watchStatus(name.replace("/tasks/", "/status/"), ctx);
                    break;
                default:
                    LOGGER.error("Error creating task: " + path + ".", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void watchStatus(String path, Object ctx) {
        ctxMap.put(path, ctx);
        zk.exists(path, statusWatcher, existsCallback, ctx);
    }

    Watcher statusWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.NodeCreated) {
                assert e.getPath().contains("/status/task-");
                assert ctxMap.containsKey(e.getPath());
                zk.getData(
                        e.getPath(),
                        false,
                        getDataCallback,
                        ctxMap.get(e.getPath()));
            }
        }
    };

    StatCallback existsCallback = new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    watchStatus(path, ctx);
                    break;
                case OK:
                    if (stat != null) {
                        zk.getData(path, false, getDataCallback, ctx);
                        LOGGER.info("Status node exists: " + path);
                    }
                    break;
                case NONODE:
                    break;
                default:
                    LOGGER.error("Error checking if status node exists: " + path + ".", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    DataCallback getDataCallback = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, getDataCallback, ctxMap.get(path));
                    return;
                case OK:
                    // print result
                    String taskResult = new String(data);
                    LOGGER.info("task " + path + ", " + taskResult);

                    // set task status
                    assert (ctx != null);
                    ((TaskObject) ctx).setStatus(taskResult.contains("done"));

                    // delete status
                    zk.delete(path, -1, taskDeleteCallback, null);
                    ctxMap.remove(path);
                    break;
                case NONODE:
                    LOGGER.warn("Status node does not exists.");
                    break;
                default:
                    LOGGER.error("Error getting data: " + path + ".", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    VoidCallback taskDeleteCallback = new VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.delete(path, -1, taskDeleteCallback, null);
                    break;
                case OK:
                    LOGGER.info("Deleted task: " + path);
                    break;
                default:
                    LOGGER.error("Error deleting task: " + path + ".", KeeperException.create(Code.get(rc), path));
            }
        }
    };

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

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    static class TaskObject {

        private String task;
        private String taskName;
        private boolean done;
        private boolean successful;
        private CountDownLatch latch;

        TaskObject() {
            this.done = false;
            this.successful = false;
            this.latch = new CountDownLatch(1);
        }

        String getTask() {
            return task;
        }

        void setTask(String task) {
            this.task = task;
        }

        String getTaskName() {
            return taskName;
        }

        void setTaskName(String taskName) {
            this.taskName = taskName;
        }

        synchronized boolean isDone() {
            return done;
        }

        synchronized boolean isSuccessful() {
            return successful;
        }

        void setStatus(boolean status) {
            this.successful = status;
            this.done = true;
            latch.countDown();
        }

        void waitUntilDone() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                LOGGER.warn("Interruped while waiting for task to complete.");
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Client c = new Client(args[0]);
        c.start();

        while (!c.isConnected()) {
            Thread.sleep(100);
        }

        TaskObject task1 = new TaskObject();
        TaskObject task2 = new TaskObject();

        c.submitTask("foo", task1);
        c.submitTask("bar", task2);

        task1.waitUntilDone();
        task2.waitUntilDone();

    }

}
