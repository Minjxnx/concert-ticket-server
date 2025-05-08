package com.concert.nameservice;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Name service implementation using etcd for service discovery.
 * This class handles registration of nodes and discovery of other nodes.
 */
public class EtcdNameService implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(EtcdNameService.class.getName());

    private static final String NODES_PREFIX = "/concert/nodes/";
    private static final long LEASE_TTL = 10; // Time to live in seconds

    private final Client etcdClient;
    private final String nodeEndpoint;
    private Long leaseId;
    private Watcher nodeWatcher;
    private List<Consumer<List<String>>> nodeChangeListeners;

    /**
     * Creates a new etcd name service
     * @param etcdEndpoints The etcd server endpoints
     * @param nodeEndpoint The endpoint of this node
     */
    public EtcdNameService(String[] etcdEndpoints, String nodeEndpoint) {
        this.etcdClient = Client.builder().endpoints(etcdEndpoints).build();
        this.nodeEndpoint = nodeEndpoint;
        this.nodeChangeListeners = new ArrayList<>();
    }

    /**
     * Registers this node with the name service
     * @throws Exception If registration fails
     */
    public void registerNode() throws Exception {
        // Create a lease
        this.leaseId = etcdClient.getLeaseClient().grant(LEASE_TTL).get().getID();

        // Start lease keep-alive
        etcdClient.getLeaseClient().keepAlive(leaseId, new io.etcd.jetcd.Observers.Observer<io.etcd.jetcd.lease.LeaseKeepAliveResponse>() {
            @Override
            public void onNext(io.etcd.jetcd.lease.LeaseKeepAliveResponse response) {
                // Keep-alive successful
            }

            @Override
            public void onError(Throwable throwable) {
                logger.severe("Lease keep-alive failed: " + throwable.getMessage());
                try {
                    // Try to re-register
                    registerNode();
                } catch (Exception e) {
                    logger.severe("Failed to re-register node: " + e.getMessage());
                }
            }

            @Override
            public void onCompleted() {
                logger.info("Lease keep-alive completed");
            }
        });

        // Register the node with the lease
        ByteSequence key = ByteSequence.from((NODES_PREFIX + nodeEndpoint).getBytes(StandardCharsets.UTF_8));
        ByteSequence value = ByteSequence.from(nodeEndpoint.getBytes(StandardCharsets.UTF_8));

        PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
        etcdClient.getKVClient().put(key, value, putOption).get();

        logger.info("Node registered with etcd: " + nodeEndpoint);
    }

    /**
     * Discovers all active nodes in the system
     * @return List of node endpoints
     * @throws Exception If discovery fails
     */
    public List<String> discoverNodes() throws Exception {
        ByteSequence prefixKey = ByteSequence.from(NODES_PREFIX.getBytes(StandardCharsets.UTF_8));
        GetOption getOption = GetOption.newBuilder().withPrefix(prefixKey).build();
        List<KeyValue> keyValues = etcdClient.getKVClient().get(prefixKey, getOption).get().getKvs();

        List<String> nodes = new ArrayList<>();
        for (KeyValue kv : keyValues) {
            String nodeEndpoint = kv.getValue().toString(StandardCharsets.UTF_8);
            nodes.add(nodeEndpoint);
        }

        logger.info("Discovered nodes: " + nodes);
        return nodes;
    }

    /**
     * Adds a listener for node changes (additions/removals)
     * @param listener The listener to add
     */
    public void addNodeChangeListener(Consumer<List<String>> listener) {
        nodeChangeListeners.add(listener);

        // If this is the first listener, start watching for changes
        if (nodeChangeListeners.size() == 1) {
            startWatchingNodes();
        }
    }

    /**
     * Starts watching for node changes
     */
    private void startWatchingNodes() {
        ByteSequence prefixKey = ByteSequence.from(NODES_PREFIX.getBytes(StandardCharsets.UTF_8));
        WatchOption watchOption = WatchOption.newBuilder().withPrefix(prefixKey).build();

        nodeWatcher = etcdClient.getWatchClient().watch(prefixKey, watchOption, response -> {
            // Node change detected
            try {
                List<String> nodes = discoverNodes();

                // Notify all listeners
                for (Consumer<List<String>> listener : nodeChangeListeners) {
                    listener.accept(nodes);
                }
            } catch (Exception e) {
                logger.severe("Error handling node change notification: " + e.getMessage());
            }
        });
    }

    /**
     * Unregisters this node from the name service
     */
    public void unregisterNode() {
        try {
            ByteSequence key = ByteSequence.from((NODES_PREFIX + nodeEndpoint).getBytes(StandardCharsets.UTF_8));
            etcdClient.getKVClient().delete(key).get();

            if (leaseId != null) {
                etcdClient.getLeaseClient().revoke(leaseId).get();
            }

            logger.info("Node unregistered from etcd: " + nodeEndpoint);
        } catch (Exception e) {
            logger.warning("Error unregistering node: " + e.getMessage());
        }
    }

    /**
     * Checks if a node is still active
     * @param nodeEndpoint The node endpoint to check
     * @return true if the node is active, false otherwise
     */
    public boolean isNodeActive(String nodeEndpoint) {
        try {
            ByteSequence key = ByteSequence.from((NODES_PREFIX + nodeEndpoint).getBytes(StandardCharsets.UTF_8));
            List<KeyValue> keyValues = etcdClient.getKVClient().get(key).get().getKvs();
            return !keyValues.isEmpty();
        } catch (Exception e) {
            logger.warning("Error checking if node is active: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        unregisterNode();

        if (nodeWatcher != null) {
            nodeWatcher.close();
        }

        if (etcdClient != null) {
            etcdClient.close();
        }
    }
}