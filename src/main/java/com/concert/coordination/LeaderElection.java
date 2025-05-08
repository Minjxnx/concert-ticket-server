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
 * Implements leader election using ZooKeeper's ephemeral sequential znodes
 */
public class LeaderElection {
    private static final Logger logger = Logger.getLogger(LeaderElection.class.getName());

    private final ZooKeeper zooKeeper;
    private final String leaderElectionPath;
    private final String nodeId;
    private final LeaderChangeListener leaderChangeListener;

    private String currentNodePath;
    private boolean isLeader = false;

    /**
     * Interface for leader change notifications
     */
    public interface LeaderChangeListener {
        /**
         * Called when the leader status changes
         * @param isLeader True if this node is now the leader, false otherwise
         * @param leaderId The ID of the current leader
         */
        void onLeaderChange(boolean isLeader, String leaderId);
    }

    /**
     * Creates a new leader election instance
     * @param zooKeeper The ZooKeeper client
     * @param leaderElectionPath The ZooKeeper path for leader election
     * @param nodeId The ID of this node
     * @param leaderChangeListener Listener for leader change events
     */
    public LeaderElection(ZooKeeper zooKeeper, String leaderElectionPath, String nodeId, LeaderChangeListener leaderChangeListener) {
        this.zooKeeper = zooKeeper;
        this.leaderElectionPath = leaderElectionPath;
        this.nodeId = nodeId;
        this.leaderChangeListener = leaderChangeListener;
    }

    /**
     * Starts the leader election process
     * @throws KeeperException If a ZooKeeper error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    public void start() throws KeeperException, InterruptedException {
        // Create an ephemeral sequential znode
        String prefix = leaderElectionPath + "/node-";
        String path = zooKeeper.create(prefix, nodeId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        currentNodePath = path;

        logger.info("Created leader election znode: " + path);

        // Determine if this node is the leader
        checkLeadership();
    }

    /**
     * Checks if this node is the leader and sets up watches
     * @throws KeeperException If a ZooKeeper error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    private void checkLeadership() throws KeeperException, InterruptedException {
        // Get all children under the leader election path
        List<String> children = zooKeeper.getChildren(leaderElectionPath, false);

        // Sort the children
        Collections.sort(children);

        // The node with the lowest sequence number is the leader
        String lowestNode = leaderElectionPath + "/" + children.get(0);
        String lowestNodeId = new String(zooKeeper.getData(lowestNode, false, null));

        // Check if this node is the leader
        boolean wasLeader = isLeader;
        isLeader = currentNodePath.endsWith(children.get(0));

        if (isLeader) {
            logger.info("This node is now the leader: " + nodeId);

            // If the leader status changed, notify the listener
            if (!wasLeader) {
                leaderChangeListener.onLeaderChange(true, nodeId);
            }
        } else {
            logger.info("Leader is: " + lowestNodeId);

            // If the leader status changed, notify the listener
            if (wasLeader) {
                leaderChangeListener.onLeaderChange(false, lowestNodeId);
            }

            // Watch the node with the next lowest sequence number
            int index = children.indexOf(currentNodePath.substring(leaderElectionPath.length() + 1));
            int watchIndex = index - 1;
            String watchPath = leaderElectionPath + "/" + children.get(watchIndex);

            logger.info("Watching node: " + watchPath);

            // Set a watch on the next lower node
            Stat stat = zooKeeper.exists(watchPath, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    try {
                        // Previous node was deleted, check leadership again
                        checkLeadership();
                    } catch (Exception e) {
                        logger.severe("Error in leader election watch: " + e.getMessage());
                    }
                }
            });

            // If the node is already gone, check leadership again
            if (stat == null) {
                checkLeadership();
            }
        }
    }

    /**
     * Stops the leader election process
     */
    public void stop() {
        try {
            if (currentNodePath != null) {
                zooKeeper.delete(currentNodePath, -1);
                currentNodePath = null;
                isLeader = false;
                leaderChangeListener.onLeaderChange(false, null);
            }
        } catch (Exception e) {
            logger.warning("Error stopping leader election: " + e.getMessage());
        }
    }

    /**
     * Checks if this node is currently the leader
     * @return true if this node is the leader, false otherwise
     */
    public boolean isLeader() {
        return isLeader;
    }
}