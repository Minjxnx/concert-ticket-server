package com.concert.server;

import com.concert.consensus.TwoPhaseCommit;
import com.concert.coordination.DistributedLock;
import com.concert.coordination.LeaderElection;
import com.concert.coordination.ZookeeperCoordinator;
import com.concert.model.Concert;
import com.concert.model.Reservation;
import com.concert.nameservice.EtcdNameService;
import com.concert.repository.DataRepository;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Represents a node in the distributed Concert Ticket Reservation System
 * Handles leadership changes and coordinates operations across the distributed system
 */
public class ServerNode {
    private static final Logger logger = LogManager.getLogger(ServerNode.class);

    private final int port;
    private final DataRepository dataRepository;
    private final ZookeeperCoordinator zookeeperCoordinator;
    private final EtcdNameService nameService;
    private final LeaderElection leaderElection;
    private final TwoPhaseCommit twoPhaseCommit;
    private final Map<String, ManagedChannel> nodeChannels;
    private boolean isPrimary;

    public ServerNode(int port, DataRepository dataRepository,
                      ZookeeperCoordinator zookeeperCoordinator,
                      EtcdNameService nameService,
                      LeaderElection leaderElection) {
        this.port = port;
        this.dataRepository = dataRepository;
        this.zookeeperCoordinator = zookeeperCoordinator;
        this.nameService = nameService;
        this.leaderElection = leaderElection;
        this.twoPhaseCommit = new TwoPhaseCommit(this);
        this.nodeChannels = new ConcurrentHashMap<>();
        this.isPrimary = false;

        // Set leadership change listener
        this.leaderElection.setLeadershipChangeListener(this::handleLeadershipChange);
    }

    /**
     * Handle leadership change
     */
    private void handleLeadershipChange(boolean isLeader) {
        this.isPrimary = isLeader;
        if (isLeader) {
            logger.info("Node on port " + port + " is now the primary node");
            // Update channels to other nodes
            refreshNodeChannels();
        } else {
            logger.info("Node on port " + port + " is now a secondary node");
        }
    }

    /**
     * Refresh channels to other nodes
     */
    public void refreshNodeChannels() {
        // Close existing channels
        for (ManagedChannel channel : nodeChannels.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Error shutting down channel", e);
            }
        }
        nodeChannels.clear();

        // Get all active nodes from name service
        Map<String, String> activeNodes = nameService.getAllNodes();
        for (Map.Entry<String, String> entry : activeNodes.entrySet()) {
            String nodeId = entry.getKey();
            String address = entry.getValue();

            // Skip self
            if (nodeId.equals("node-" + port)) {
                continue;
            }

            // Create channel
            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            nodeChannels.put(nodeId, channel);
            logger.info("Created channel to node: " + nodeId + " at " + address);
        }
    }

    /**
     * Add a new concert
     */
    public void addConcert(Concert concert, StreamObserver<Boolean> responseObserver) {
        if (isPrimary) {
            // Acquire lock for concert operations
            DistributedLock lock = new DistributedLock(zookeeperCoordinator, "/concert_lock");
            try {
                // Acquire lock
                lock.acquire();

                // Initiate two-phase commit
                boolean result = twoPhaseCommit.coordinateAddConcert(concert);

                // Return result
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error adding concert", e);
                responseObserver.onNext(false);
                responseObserver.onCompleted();
            } finally {
                // Release lock
                lock.release();
            }
        } else {
            // Forward to primary node
            forwardAddConcertToPrimary(concert, responseObserver);
        }
    }

    /**
     * Forward add concert request to primary node
     */
    private void forwardAddConcertToPrimary(Concert concert, StreamObserver<Boolean> responseObserver) {
        // Implementation to forward request to primary node
        // Will be completed when we implement the gRPC stubs
        logger.info("Forwarding add concert request to primary node");

        // For now, just return false
        responseObserver.onNext(false);
        responseObserver.onCompleted();
    }

    /**
     * Update concert details
     */
    public void updateConcert(Concert concert, StreamObserver<Boolean> responseObserver) {
        if (isPrimary) {
            // Acquire lock for concert operations
            DistributedLock lock = new DistributedLock(zookeeperCoordinator, "/concert_lock");
            try {
                // Acquire lock
                lock.acquire();

                // Initiate two-phase commit
                boolean result = twoPhaseCommit.coordinateUpdateConcert(concert);

                // Return result
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error updating concert", e);
                responseObserver.onNext(false);
                responseObserver.onCompleted();
            } finally {
                // Release lock
                lock.release();
            }
        } else {
            // Forward to primary node
            forwardUpdateConcertToPrimary(concert, responseObserver);
        }
    }

    /**
     * Forward update concert request to primary node
     */
    private void forwardUpdateConcertToPrimary(Concert concert, StreamObserver<Boolean> responseObserver) {
        // Implementation to forward request to primary node
        // Will be completed when we implement the gRPC stubs
        logger.info("Forwarding update concert request to primary node");

        // For now, just return false
        responseObserver.onNext(false);
        responseObserver.onCompleted();
    }

    /**
     * Cancel a concert
     */
    public void cancelConcert(String concertId, StreamObserver<Boolean> responseObserver) {
        if (isPrimary) {
            // Acquire lock for concert operations
            DistributedLock lock = new DistributedLock(zookeeperCoordinator, "/concert_lock");
            try {
                // Acquire lock
                lock.acquire();

                // Initiate two-phase commit
                boolean result = twoPhaseCommit.coordinateCancelConcert(concertId);

                // Return result
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error canceling concert", e);
                responseObserver.onNext(false);
                responseObserver.onCompleted();
            } finally {
                // Release lock
                lock.release();
            }
        } else {
            // Forward to primary node
            forwardCancelConcertToPrimary(concertId, responseObserver);
        }
    }

    /**
     * Forward cancel concert request to primary node
     */
    private void forwardCancelConcertToPrimary(String concertId, StreamObserver<Boolean> responseObserver) {
        // Implementation to forward request to primary node
        // Will be completed when we implement the gRPC stubs
        logger.info("Forwarding cancel concert request to primary node");

        // For now, just return false
        responseObserver.onNext(false);
        responseObserver.onCompleted();
    }

    /**
     * Update ticket stock
     */
    public void updateTicketStock(String concertId, String seatType, int quantity, StreamObserver<Boolean> responseObserver) {
        if (isPrimary) {
            // Acquire lock for ticket operations
            DistributedLock lock = new DistributedLock(zookeeperCoordinator, "/ticket_lock");
            try {
                // Acquire lock
                lock.acquire();

                // Initiate two-phase commit
                boolean result = twoPhaseCommit.coordinateUpdateTicketStock(concertId, seatType, quantity);

                // Return result
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error updating ticket stock", e);
                responseObserver.onNext(false);
                responseObserver.onCompleted();
            } finally {
                // Release lock
                lock.release();
            }
        } else {
            // Forward to primary node
            forwardUpdateTicketStockToPrimary(concertId, seatType, quantity, responseObserver);
        }
    }

    /**
     * Forward update ticket stock request to primary node
     */
    private void forwardUpdateTicketStockToPrimary(String concertId, String seatType, int quantity, StreamObserver<Boolean> responseObserver) {
        // Implementation to forward request to primary node
        // Will be completed when we implement the gRPC stubs
        logger.info("Forwarding update ticket stock request to primary node");

        // For now, just return false
        responseObserver.onNext(false);
        responseObserver.onCompleted();
    }

    /**
     * Make a reservation
     */
    public void makeReservation(Reservation reservation, StreamObserver<Boolean> responseObserver) {
        if (isPrimary) {
            // Acquire lock for reservation operations
            DistributedLock lock = new DistributedLock(zookeeperCoordinator, "/reservation_lock");
            try {
                // Acquire lock
                lock.acquire();

                // Initiate two-phase commit
                boolean result = twoPhaseCommit.coordinateMakeReservation(reservation);

                // Return result
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error making reservation", e);
                responseObserver.onNext(false);
                responseObserver.onCompleted();
            } finally {
                // Release lock
                lock.release();
            }
        } else {
            // Forward to primary node
            forwardMakeReservationToPrimary(reservation, responseObserver);
        }
    }

    /**
     * Forward make reservation request to primary node
     */
    private void forwardMakeReservationToPrimary(Reservation reservation, StreamObserver<Boolean> responseObserver) {
        // Implementation to forward request to primary node
        // Will be completed when we implement the gRPC stubs
        logger.info("Forwarding make reservation request to primary node");

        // For now, just return false
        responseObserver.onNext(false);
        responseObserver.onCompleted();
    }

    /**
     * Get repository
     */
    public DataRepository getDataRepository() {
        return dataRepository;
    }

    /**
     * Get node channels
     */
    public Map<String, ManagedChannel> getNodeChannels() {
        return nodeChannels;
    }

    /**
     * Check if node is primary
     */
    public boolean isPrimary() {
        return isPrimary;
    }

    /**
     * Get port
     */
    public int getPort() {
        return port;
    }
}