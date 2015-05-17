import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.StatCallback;
import static org.apache.zookeeper.AsyncCallback.StringCallback;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.KeeperException.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Worker implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    public static String WORKERS_PATH_PREFIX = "/workers";
    private static String WORKER = "worker";
    private static String IDLE = "Idle";

    ZooKeeper zk;
    String hostPort;
    String serverId;
    String status;
    String name;

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
        this.name = WORKER + "-" + serverId;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent e) {
        LOGGER.info("Process: " + e + ", " + hostPort);
    }

    void register() {
        zk.create(WORKERS_PATH_PREFIX + "/" + name,
            IDLE.getBytes(),
            OPEN_ACL_UNSAFE,
            EPHEMERAL,
            createWorkerCallback,
            null
        );
    }

    synchronized void updateStatus(String status) {
        if (this.status == status) {
            zk.setData(WORKERS_PATH_PREFIX + "/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker(args[0]);
        w.startZk();
        w.register();

        Thread.sleep(100000);
    }

}
