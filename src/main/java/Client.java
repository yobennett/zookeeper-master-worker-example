import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;
import static org.apache.zookeeper.KeeperException.ConnectionLossException;
import static org.apache.zookeeper.KeeperException.NodeExistsException;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Client implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    ZooKeeper zk;
    String hostPort;

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
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

    public void process(WatchedEvent e) {
        LOGGER.info("Process: " + e + ", " + hostPort);
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client(args[0]);
        c.startZk();
        String name = c.queueCommand(args[1]);
        LOGGER.info("created: " + name);
    }

}
