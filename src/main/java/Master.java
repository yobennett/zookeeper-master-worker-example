import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.KeeperException.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {

    private static String MASTER_PATH = "/master";

    ZooKeeper zk;
    String hostPort;
    String serverId;
    boolean isLeader;

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

    private boolean checkMaster() throws KeeperException, InterruptedException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData(MASTER_PATH, false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (NoNodeException e) {
                // no master
                return false;
            } catch (ConnectionLossException e) {
                // keep trying to get data for master
            }
        }
    }

    void runForMaster() throws KeeperException, InterruptedException {
        while (true) {
            try {
                zk.create(MASTER_PATH, serverId.getBytes(), OPEN_ACL_UNSAFE, EPHEMERAL);
                isLeader = true;
                break;
            } catch (NodeExistsException e) {
                // master already exists
                isLeader = false;
                break;
            } catch (ConnectionLossException e) {
                // keep trying to create master
            }
            if (checkMaster()) {
                break;
            }
        }
    }

    boolean isLeader() {
        return isLeader;
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master(args[0]);
        m.startZk();

        m.runForMaster();

        if (m.isLeader()) {
            System.out.println("I'm the leader.");
            Thread.sleep(60000); // wait
        } else {
            System.out.println("Someone else is the leader.");
        }

        m.stopZk();
    }

}
