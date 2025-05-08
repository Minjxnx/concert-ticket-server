
package com.concert.consensus;

import com.concert.server.ServerNode;
import com.concert.proto.CommitRequest;
import com.concert.proto.CommitResponse;
import com.concert.proto.PrepareRequest;
import com.concert.proto.PrepareResponse;
import com.concert.proto.TransactionData;
import com.concert.proto.Operation;
import com.concert.proto.SystemCoordinationServiceGrpc;
import com.concert.coordination.DistributedLock;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Implementation of the Two-Phase Commit protocol for ensuring atomicity
 * across multiple distributed operations.
 */
public class TwoPhaseCommit {
    private static final Logger logger = Logger.getLogger(TwoPhaseCommit.class.getName());

    private final ServerNode serverNode;
    private final Map<String, TransactionData> pendingTransactions;

    public TwoPhaseCommit(ServerNode serverNode) {
        this.serverNode = serverNode;
        this.pendingTransactions = new ConcurrentHashMap<>();
    }

    /**
     * Start a two-phase commit process as the coordinator
     * @param operation The operation type
     * @param data The serialized data for the operation
     * @return true if the transaction was committed successfully, false otherwise
     */
    public boolean coordinateTransaction(Operation operation, byte[] data) {
        String transactionId = UUID.randomUUID().toString();

        // Create transaction data
        TransactionData transactionData = TransactionData.newBuilder()
                .setTransactionId(transactionId)
                .setOperation(operation)
                .setData(com.google.protobuf.ByteString.copyFrom(data))
                .build();

        // Store transaction locally
        pendingTransactions.put(transactionId, transactionData);

        // Phase 1: Prepare
        boolean allPrepared = sendPrepareRequests(transactionData);

        if (!allPrepared) {
            // If any node votes no, abort the transaction
            sendAbortRequests(transactionId);
            pendingTransactions.remove(transactionId);
            logger.info("Transaction " + transactionId + " aborted in prepare phase");
            return false;
        }

        // Phase 2: Commit
        boolean allCommitted = sendCommitRequests(transactionId);

        pendingTransactions.remove(transactionId);

        if (!allCommitted) {
            logger.warning("Transaction " + transactionId + " partially committed - system may be in inconsistent state");
            return false;
        }

        logger.info("Transaction " + transactionId + " successfully committed");
        return true;
    }

    /**
     * Process a prepare request as a participant
     * @param request The prepare request
     * @return true if prepared successfully, false otherwise
     */
    public boolean processPrepareRequest(PrepareRequest request) {
        String transactionId = request.getTransactionData().getTransactionId();
        Operation operation = request.getTransactionData().getOperation();
        byte[] data = request.getTransactionData().getData().toByteArray();

        // Check if we can perform this operation
        boolean canPrepare = checkOperationFeasibility(operation, data);

        if (canPrepare) {
            // Store the transaction for later commit/abort
            pendingTransactions.put(transactionId, request.getTransactionData());
            logger.info("Node prepared for transaction: " + transactionId);
        } else {
            logger.info("Node cannot prepare for transaction: " + transactionId);
        }

        return canPrepare;
    }

    /**
     * Process a commit request as a participant
     * @param transactionId The transaction ID to commit
     * @return true if committed successfully, false otherwise
     */
    public boolean processCommitRequest(String transactionId) {
        TransactionData transactionData = pendingTransactions.get(transactionId);

        if (transactionData == null) {
            logger.warning("Cannot commit unknown transaction: " + transactionId);
            return false;
        }

        boolean committed = executeOperation(transactionData.getOperation(), transactionData.getData().toByteArray());

        if (committed) {
            pendingTransactions.remove(transactionId);
            logger.info("Transaction committed: " + transactionId);
        } else {
            logger.severe("Failed to commit transaction: " + transactionId);
        }

        return committed;
    }

    /**
     * Process an abort request as a participant
     * @param transactionId The transaction ID to abort
     */
    public void processAbortRequest(String transactionId) {
        pendingTransactions.remove(transactionId);
        logger.info("Transaction aborted: " + transactionId);
    }

    // Helper methods

    private boolean sendPrepareRequests(TransactionData transactionData) {
        Map<String, Boolean> prepareResults = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(serverNode.getActiveNodeEndpoints().size());

        // Create prepare request
        PrepareRequest prepareRequest = PrepareRequest.newBuilder()
                .setTransactionData(transactionData)
                .build();

        // Send prepare request to all participants (including self)
        for (String endpoint : serverNode.getActiveNodeEndpoints()) {
            if (endpoint.equals(serverNode.getEndpoint())) {
                // Process locally
                boolean prepared = processPrepareRequest(prepareRequest);
                prepareResults.put(endpoint, prepared);
                latch.countDown();
            } else {
                // Send to remote node
                ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint)
                        .usePlaintext()
                        .build();

                SystemCoordinationServiceGrpc.SystemCoordinationServiceStub stub =
                        SystemCoordinationServiceGrpc.newStub(channel);

                stub.prepare(prepareRequest, new StreamObserver<PrepareResponse>() {
                    @Override
                    public void onNext(PrepareResponse response) {
                        prepareResults.put(endpoint, response.getPrepared());
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.warning("Error sending prepare to " + endpoint + ": " + t.getMessage());
                        prepareResults.put(endpoint, false);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });
            }
        }

        try {
            // Wait for all responses with timeout
            boolean allResponded = latch.await(5, TimeUnit.SECONDS);
            if (!allResponded) {
                logger.warning("Timeout waiting for prepare responses");
                return false;
            }
        } catch (InterruptedException e) {
            logger.warning("Interrupted while waiting for prepare responses: " + e.getMessage());
            return false;
        }

        // Check if all participants voted yes
        return !prepareResults.containsValue(false);
    }

    private boolean sendCommitRequests(String transactionId) {
        Map<String, Boolean> commitResults = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(serverNode.getActiveNodeEndpoints().size());

        // Create commit request
        CommitRequest commitRequest = CommitRequest.newBuilder()
                .setTransactionId(transactionId)
                .build();

        // Send commit request to all participants (including self)
        for (String endpoint : serverNode.getActiveNodeEndpoints()) {
            if (endpoint.equals(serverNode.getEndpoint())) {
                // Process locally
                boolean committed = processCommitRequest(transactionId);
                commitResults.put(endpoint, committed);
                latch.countDown();
            } else {
                // Send to remote node
                ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint)
                        .usePlaintext()
                        .build();

                SystemCoordinationServiceGrpc.SystemCoordinationServiceStub stub =
                        SystemCoordinationServiceGrpc.newStub(channel);

                stub.commit(commitRequest, new StreamObserver<CommitResponse>() {
                    @Override
                    public void onNext(CommitResponse response) {
                        commitResults.put(endpoint, response.getCommitted());
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.warning("Error sending commit to " + endpoint + ": " + t.getMessage());
                        commitResults.put(endpoint, false);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });
            }
        }

        try {
            // Wait for all responses with timeout
            boolean allResponded = latch.await(5, TimeUnit.SECONDS);
            if (!allResponded) {
                logger.warning("Timeout waiting for commit responses");
                return false;
            }
        } catch (InterruptedException e) {
            logger.warning("Interrupted while waiting for commit responses: " + e.getMessage());
            return false;
        }

        // Check if all participants committed successfully
        return !commitResults.containsValue(false);
    }

    private void sendAbortRequests(String transactionId) {
        // Create abort request
        CommitRequest abortRequest = CommitRequest.newBuilder()
                .setTransactionId(transactionId)
                .build();

        // Send abort request to all participants (including self)
        for (String endpoint : serverNode.getActiveNodeEndpoints()) {
            if (endpoint.equals(serverNode.getEndpoint())) {
                // Process locally
                processAbortRequest(transactionId);
            } else {
                // Send to remote node
                ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint)
                        .usePlaintext()
                        .build();

                SystemCoordinationServiceGrpc.SystemCoordinationServiceStub stub =
                        SystemCoordinationServiceGrpc.newStub(channel);

                stub.abort(abortRequest, new StreamObserver<CommitResponse>() {
                    @Override
                    public void onNext(CommitResponse response) {
                        // Ignore response for abort
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.warning("Error sending abort to " + endpoint + ": " + t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        // Do nothing
                    }
                });
            }
        }
    }

    // These methods interface with the actual business logic

    private boolean checkOperationFeasibility(Operation operation, byte[] data) {
        try {
            DistributedLock lock = serverNode.getDistributedLock();
            lock.lock();

            try {
                switch (operation) {
                    case ADD_CONCERT:
                        // Always feasible to add a new concert
                        return true;

                    case UPDATE_CONCERT:
                        // Check if concert exists
                        // Deserialize data and check if update is possible
                        return serverNode.getServiceImpl().canUpdateConcert(data);

                    case CANCEL_CONCERT:
                        // Check if concert exists and not already canceled
                        return serverNode.getServiceImpl().canCancelConcert(data);

                    case MAKE_RESERVATION:
                        // Check if seats and after-party tickets are available
                        return serverNode.getServiceImpl().canMakeReservation(data);

                    default:
                        logger.warning("Unknown operation type: " + operation);
                        return false;
                }
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            logger.severe("Error checking operation feasibility: " + e.getMessage());
            return false;
        }
    }

    private boolean executeOperation(Operation operation, byte[] data) {
        try {
            DistributedLock lock = serverNode.getDistributedLock();
            lock.lock();

            try {
                switch (operation) {
                    case ADD_CONCERT:
                        return serverNode.getServiceImpl().executeAddConcert(data);

                    case UPDATE_CONCERT:
                        return serverNode.getServiceImpl().executeUpdateConcert(data);

                    case CANCEL_CONCERT:
                        return serverNode.getServiceImpl().executeCancelConcert(data);

                    case MAKE_RESERVATION:
                        return serverNode.getServiceImpl().executeMakeReservation(data);

                    default:
                        logger.warning("Unknown operation type: " + operation);
                        return false;
                }
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            logger.severe("Error executing operation: " + e.getMessage());
            return false;
        }
    }
}