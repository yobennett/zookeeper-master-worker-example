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

    Master(String hostPort) {
        this.hostPort = hostPort;
        this.serverId = Long.toString(new Random().nextLong());
        this.isLeader = false;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
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
