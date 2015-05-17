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
    private volatile MasterStates state;

    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.
     */
    enum MasterStates {
        RUNNING,
        ELECTED,
        NOTELECTED
    }

    // callbacks

    StringCallback masterCreateCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
//                    isLeader = true;
                    state = MasterStates.ELECTED;
                    takeLeadership();
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                default:
//                    isLeader = false;
                    state = MasterStates.NOTELECTED;
                    LOGGER.error("Error while running for master.", KeeperException.create(Code.get(rc), path));
                    break;
            }
            LOGGER.info("I'm " + (isLeader ? "" : "not ") + "the leader");
        }
    };

    StatCallback masterExistsCallback = new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    runForMaster();
                    LOGGER.info("Previous master was deleted. Run for master again.");
                    break;
                default:
                    checkMaster();
                    break;
            }
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

    // watchers

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.NodeDeleted) {
                assert MASTER_PATH.equals(e.getPath());
                runForMaster();
            }
        }
    };

    Master(String hostPort) {
        this.hostPort = hostPort;
        this.serverId = Long.toString(new Random().nextLong());
        this.connected = false;
        this.expired = false;
        this.state = MasterStates.RUNNING;
        isLeader = false;
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

    void masterExists() {
        zk.exists(MASTER_PATH, masterExistsWatcher, masterExistsCallback, null);
    }

    void takeLeadership() {
        LOGGER.info("Taking leadership.");
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
