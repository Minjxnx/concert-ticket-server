package com.concert.coordination;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Coordinator for ZooKeeper-based services including lock management
 * and leader election.
 */
public class ZookeeperCoordinator implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ZookeeperCoordinator.class.getName());

    private static final String ZNODE_ROOT = "/concert";
    private static final String ZNODE_LEADER_ELECTION = ZNODE_ROOT + "/leader";
    private static final String ZNODE_LOCKS = ZNODE_ROOT + "/locks";

    private final ZooKeeper zooKeeper;
    private final String connectString;
    private final int sessionTimeout;
    private final String nodeId;

    /**
     * Creates a new ZooKeeper coordinator
     * @param connectString ZooKeeper connection string (host:port,host:port,...)
     * @param sessionTimeout Session timeout in milliseconds
     * @param nodeId Unique identifier for this node
     * @throws IOException If connection to ZooKeeper fails
     */
    public ZookeeperCoordinator(String connectString, int sessionTimeout, String nodeId) throws IOException {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        this.nodeId = nodeId;

        CountDownLatch connectionLatch = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(connectString, sessionTimeout, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
        });

        try {
            connectionLatch.await();
            initializeZnodes();
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while connecting to ZooKeeper", e);
        }
    }

    /**
     * Initializes required ZooKeeper znodes
     */
    private void initializeZnodes() {
        try {
            // Create root znode if it doesn't exist
            createZNodeIfNotExists(ZNODE_ROOT, new byte[0], CreateMode.PERSISTENT);

            // Create leader election znode if it doesn't exist
            createZNodeIfNotExists(ZNODE_LEADER_ELECTION, new byte[0], CreateMode.PERSISTENT);

            // Create locks znode if it doesn't exist
            createZNodeIfNotExists(ZNODE_LOCKS, new byte[0], CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            logger.severe("Error initializing ZooKeeper znodes: " + e.getMessage());
        }
    }

    /**
     * Creates a ZooKeeper znode if it doesn't already exist
     * @param path The znode path
     * @param data The data to store in the znode
     * @param createMode The znode creation mode
     * @throws KeeperException If a ZooKeeper error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    private void createZNodeIfNotExists(String path, byte[] data, CreateMode createMode)
            throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            String createdPath = zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            logger.info("Created ZooKeeper znode: " + createdPath);
        }
    }

    /**
     * Creates a distributed lock instance
     * @param lockName The name of the lock
     * @return A distributed lock
     */
    public DistributedLock createLock(String lockName) {
        return new DistributedLock(zooKeeper, ZNODE_LOCKS + "/" + lockName, nodeId);
    }

    /**
     * Creates a leader election instance
     * @param leaderChangeListener Listener to notify on leader changes
     * @return A leader election instance
     */
    public LeaderElection createLeaderElection(LeaderElection.LeaderChangeListener leaderChangeListener) {
        return new LeaderElection(zooKeeper, ZNODE_LEADER_ELECTION, nodeId, leaderChangeListener);
    }

    /**
     * Gets the current ZooKeeper session
     * @return The ZooKeeper instance
     */
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    @Override
    public void close() {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            logger.warning("Error closing ZooKeeper connection: " + e.getMessage());
        }
    }
}