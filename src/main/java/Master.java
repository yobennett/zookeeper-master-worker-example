import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.DataCallback;
import static org.apache.zookeeper.AsyncCallback.StringCallback;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.KeeperException.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Master.class);
    private static String MASTER_PATH = "/master";
    static boolean isLeader;

    ZooKeeper zk;
    String hostPort;
    String serverId;

    StringCallback masterCreateCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            LOGGER.info("I'm " + (isLeader ? "" : "not ") + "the leader");
        }
    };

    DataCallback masterCheckCallback = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case NONODE:
                    runForMaster();
                    break;
                default:
                    LOGGER.error("Can't read data. ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StringCallback createParentCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    LOGGER.info("Parent created: " + path);
                    break;
                case NODEEXISTS:
                    LOGGER.warn("Parent already registered: " + path);
                    break;
                default:
                    LOGGER.error("Error creating parent: " + path + ".", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    Master(String hostPort) {
        this.hostPort = hostPort;
        this.serverId = Long.toString(new Random().nextLong());
        this.isLeader = false;
    }

    void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path, data, OPEN_ACL_UNSAFE, PERSISTENT, createParentCallback, data);
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    public void process(WatchedEvent e) {
        LOGGER.info("Process: " + e);
    }

    private void checkMaster() {
        zk.getData(MASTER_PATH, false, masterCheckCallback, null);
    }

    void runForMaster() {
        zk.create(MASTER_PATH, serverId.getBytes(), OPEN_ACL_UNSAFE, EPHEMERAL, masterCreateCallback, null);
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master(args[0]);
        m.startZk();
        // TODO implement async state tests
        m.stopZk();
    }

}
