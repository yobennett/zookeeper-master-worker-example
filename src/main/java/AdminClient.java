import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

import static org.apache.zookeeper.KeeperException.*;

public class AdminClient implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClient.class);

    ZooKeeper zk;
    String hostPort;

    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent e) {
        LOGGER.info("Process: " + e + ", " + hostPort);
    }

    void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            LOGGER.info("Master: " + new String(masterData) + " since " + startDate);
        } catch (NoNodeException e) {
            LOGGER.info("No master");
        }

        LOGGER.info("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            LOGGER.info("\t" + w + ":" + state);
        }

        LOGGER.info("Tasks:");
        for (String t : zk.getChildren("/assign", false)) {
            LOGGER.info("\t" + t);
        }

    }

    public static void main(String[] args) throws Exception {
        AdminClient c = new AdminClient(args[0]);
        c.startZk();
        c.listState();
    }

}
