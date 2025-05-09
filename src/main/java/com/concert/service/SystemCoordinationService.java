package com.concert.service;

import com.concert.model.Concert;
import com.concert.model.Reservation;
import com.concert.repository.DataRepository;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

/**
 * Service implementation for system coordination between nodes.
 * Handles operations like data synchronization, leader election, and two-phase commit protocol.
 */
public class SystemCoordinationService extends SystemCoordinationServiceGrpc.SystemCoordinationServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(SystemCoordinationService.class);

    private final DataRepository dataRepository;

    public SystemCoordinationService(DataRepository dataRepository) {
        this.dataRepository = dataRepository;
    }

    @Override
    public void prepareCommit(PrepareCommitRequest request,
                              StreamObserver<PrepareCommitResponse> responseObserver) {
        try {
            // Deserialize data
            Map<String, Object> data = deserializeData(request.getData().toByteArray());

            // Check operation type and delegate to appropriate service
            String operation = (String) data.get("operation");
            boolean canCommit = false;

            if (operation.startsWith("ADD_CONCERT") ||
                    operation.startsWith("UPDATE_CONCERT") ||
                    operation.startsWith("CANCEL_CONCERT")) {
                // Concert operations
                ConcertService concertService = new ConcertService(dataRepository, null, null, false);
                canCommit = concertService.prepareCommit(data);
            } else if (operation.startsWith("MAKE_RESERVATION") ||
                    operation.startsWith("BULK_RESERVATION")) {
                // Reservation operations
                ReservationService reservationService = new ReservationService(dataRepository, null, null, false);
                canCommit = reservationService.prepareCommit(data);
            } else if (operation.startsWith("UPDATE_AFTER_PARTY_TICKETS") ||
                    operation.startsWith("UPDATE_SEAT_TIER") ||
                    operation.startsWith("UPDATE_TICKET_PRICE")) {
                // Ticket inventory operations
                TicketInventoryService ticketService = new TicketInventoryService(dataRepository, null, null, false);
                canCommit = ticketService.prepareCommit(data);
            } else {
                logger.warn("Unknown operation in prepare commit: " + operation);
                canCommit = false;
            }

            // Send response
            PrepareCommitResponse response = PrepareCommitResponse.newBuilder()
                    .setTransactionId(request.getTransactionId())
                    .setCanCommit(canCommit)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error in prepare commit", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitResponse> responseObserver) {
        try {
            // Deserialize data
            Map<String, Object> data = deserializeData(request.getData().toByteArray());

            // Check operation type and delegate to appropriate service
            String operation = (String) data.get("operation");
            boolean success = false;

            if (operation.startsWith("ADD_CONCERT") ||
                    operation.startsWith("UPDATE_CONCERT") ||
                    operation.startsWith("CANCEL_CONCERT")) {
                // Concert operations
                ConcertService concertService = new ConcertService(dataRepository, null, null, false);
                success = concertService.applyCommit(data);
            } else if (operation.startsWith("MAKE_RESERVATION") ||
                    operation.startsWith("BULK_RESERVATION")) {
                // Reservation operations
                ReservationService reservationService = new ReservationService(dataRepository, null, null, false);
                success = reservationService.applyCommit(data);
            } else if (operation.startsWith("UPDATE_AFTER_PARTY_TICKETS") ||
                    operation.startsWith("UPDATE_SEAT_TIER") ||
                    operation.startsWith("UPDATE_TICKET_PRICE")) {
                // Ticket inventory operations
                TicketInventoryService ticketService = new TicketInventoryService(dataRepository, null, null, false);
                success = ticketService.applyCommit(data);
            } else {
                logger.warn("Unknown operation in commit: " + operation);
                success = false;
            }

            // Send response
            CommitResponse response = CommitResponse.newBuilder()
                    .setTransactionId(request.getTransactionId())
                    .setSuccess(success)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error in commit", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void abort(AbortRequest request, StreamObserver<AbortResponse> responseObserver) {
        try {
            // Nothing to do for abort, as changes are only applied during commit
            // Just acknowledge the abort
            AbortResponse response = AbortResponse.newBuilder()
                    .setTransactionId(request.getTransactionId())
                    .setSuccess(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error in abort", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void syncData(SyncDataRequest request, StreamObserver<SyncDataResponse> responseObserver) {
        try {
            // This method is called when a node rejoins the cluster or
            // when a new node joins and needs to sync data from the primary

            // Serialize all concerts
            List<Concert> allConcerts = dataRepository.getAllConcerts();
            byte[] concertData = serializeObject(allConcerts);

            // Serialize all reservations
            List<Reservation> allReservations = dataRepository.getAllReservations();
            byte[] reservationData = serializeObject(allReservations);

            // Build response
            SyncDataResponse response = SyncDataResponse.newBuilder()
                    .setConcertData(ByteString.copyFrom(concertData))
                    .setReservationData(ByteString.copyFrom(reservationData))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error in data sync", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        try {
            // Simple heartbeat to check if node is alive
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setStatus("ok")
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error in heartbeat", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void leaderInfo(LeaderInfoRequest request, StreamObserver<LeaderInfoResponse> responseObserver) {
        try {
            // When a node needs to know who the current leader/primary is
            // This would be filled with the actual leader data from your leader election mechanism
            LeaderInfoResponse response = LeaderInfoResponse.newBuilder()
                    .setLeaderId("node1") // Example, this would be dynamically determined
                    .setLeaderAddress("localhost:9090") // Example
                    .setLeaderSince(System.currentTimeMillis()) // Example
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error in leader info", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    // Helper method to serialize objects
    private byte[] serializeObject(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return baos.toByteArray();
    }

    // Helper method to deserialize data
    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializeData(byte[] data) throws IOException, ClassNotFoundException {
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
        java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
        return (Map<String, Object>) ois.readObject();
    }
}