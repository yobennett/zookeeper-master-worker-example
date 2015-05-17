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
    boolean connected;
    boolean expired;

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
        this.connected = false;
        this.expired = false;
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
        zk = new ZooKeeper(hostPort, 150000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    public void process(WatchedEvent e) {
        LOGGER.info("Process: " + e);
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

    private void checkMaster() {
        zk.getData(MASTER_PATH, false, masterCheckCallback, null);
    }

    void runForMaster() {
        zk.create(MASTER_PATH, serverId.getBytes(), OPEN_ACL_UNSAFE, EPHEMERAL, masterCreateCallback, null);
    }

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master(args[0]);
        m.startZk();

        while(!m.isConnected()) {
            Thread.sleep(100);
        }

        m.bootstrap();
        m.runForMaster();

        while(!m.isExpired()) {
            Thread.sleep(1000);
        }

        m.stopZk();
    }

}
