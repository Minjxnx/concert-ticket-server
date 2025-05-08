package com.concert.coordination;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Distributed lock implementation using ZooKeeper
 */
public class DistributedLock {
    private static final Logger logger = Logger.getLogger(DistributedLock.class.getName());

    private final ZooKeeper zooKeeper;
    private final String lockPath;
    private final String nodeId;
    private String lockNodePath;

    /**
     * Creates a new distributed lock
     * @param zooKeeper The ZooKeeper client
     * @param lockPath The ZooKeeper path for the lock
     * @param nodeId The ID of this node
     */
    public DistributedLock(ZooKeeper zooKeeper, String lockPath, String nodeId) {
        this.zooKeeper = zooKeeper;
        this.lockPath = lockPath;
        this.nodeId = nodeId;

        // Ensure the lock path exists
        try {
            Stat stat = zooKeeper.exists(lockPath, false);
            if (stat == null) {
                zooKeeper.create(lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            logger.warning("Error creating lock path: " + e.getMessage());
        }
    }

    /**
     * Acquires the distributed lock
     * @throws KeeperException If a ZooKeeper error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    public void lock() throws KeeperException, InterruptedException {
        // Create an ephemeral sequential znode
        String prefix = lockPath + "/lock-";
        String path = zooKeeper.create(prefix, nodeId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        lockNodePath = path;

        logger.fine("Created lock node: " + path);

        // Keep trying until we acquire the lock
        while (true) {
            // Get all children under the lock path
            List<String> children = zooKeeper.getChildren(lockPath, false);

            // Sort the children
            Collections.sort(children);

            // Get the name of our lock node (without the prefix)
            String lockNode = path.substring(lockPath.length() + 1);

            // Check if our node is the lowest in the sequence (has the lock)
            int index = children.indexOf(lockNode);
            if (index == 0) {
                // We have the lock
                logger.fine("Acquired lock: " + path);
                return;
            }

            // Watch the node with the next lowest sequence number
            String watchPath = lockPath + "/" + children.get(index - 1);

            logger.fine("Waiting for lock, watching: " + watchPath);

            // Wait for the previous node to be deleted
            CountDownLatch latch = new CountDownLatch(1);
            Stat stat = zooKeeper.exists(watchPath, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    latch.countDown();
                }
            });

            // If the node disappeared between the getChildren and the exists call, try again
            if (stat == null) {
                continue;
            }

            // Wait for the latch to be released
            latch.await();
        }
    }

    /**
     * Releases the distributed lock
     * @throws KeeperException If a ZooKeeper error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    public void unlock() throws KeeperException, InterruptedException {
        if (lockNodePath != null) {
            try {
                zooKeeper.delete(lockNodePath, -1);
                lockNodePath = null;
                logger.fine("Released lock: " + lockNodePath);
            } catch (KeeperException.NoNodeException e) {
                // Node already deleted, perhaps due to session expiration
                lockNodePath = null;
            }
        }
    }

    /**
     * Checks if this node currently holds the lock
     * @return true if this node holds the lock, false otherwise
     * @throws KeeperException If a ZooKeeper error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    public boolean isHeldByCurrentNode() throws KeeperException, InterruptedException {
        if (lockNodePath == null) {
            return false;
        }

        // Get all children under the lock path
        List<String> children = zooKeeper.getChildren(lockPath, false);

        if (children.isEmpty()) {
            return false;
        }

        // Sort the children
        Collections.sort(children);

        // Get the name of our lock node (without the prefix)
        String lockNode = lockNodePath.substring(lockPath.length() + 1);

        // Check if our node is the lowest in the sequence (has the lock)
        return children.get(0).equals(lockNode);
    }
}