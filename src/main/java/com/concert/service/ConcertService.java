package com.concert.service;

import com.concert.consensus.TwoPhaseCommit;
import com.concert.coordination.DistributedLock;
import com.concert.model.Concert;
import com.concert.model.Seat;
import com.concert.repository.DataRepository;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service implementation for managing concerts in the distributed system.
 * Handles operations like adding, updating, and canceling concerts.
 */
public class ConcertService extends ConcertServiceGrpc.ConcertServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ConcertService.class);

    private final DataRepository dataRepository;
    private final TwoPhaseCommit twoPhaseCommit;
    private final DistributedLock distributedLock;
    private final boolean isPrimary;

    public ConcertService(DataRepository dataRepository, TwoPhaseCommit twoPhaseCommit,
                          DistributedLock distributedLock, boolean isPrimary) {
        this.dataRepository = dataRepository;
        this.twoPhaseCommit = twoPhaseCommit;
        this.distributedLock = distributedLock;
        this.isPrimary = isPrimary;
    }

    @Override
    public void addConcert(AddConcertRequest request, StreamObserver<AddConcertResponse> responseObserver) {
        if (!isPrimary) {
            // Forward request to primary node if this is a secondary node
            forwardToPrimary(request, responseObserver);
            return;
        }

        try {
            // Acquire distributed lock to ensure consistency
            distributedLock.acquireLock("concert");

            // Generate a unique ID for the new concert
            String concertId = UUID.randomUUID().toString();

            // Create a Concert object from the request
            Concert concert = new Concert();
            concert.setId(concertId);
            concert.setName(request.getName());
            concert.setDate(new Date(request.getDate()));
            concert.setVenue(request.getVenue());
            concert.setAfterPartyTicketsTotal(request.getAfterPartyTicketsTotal());
            concert.setAfterPartyTicketsAvailable(request.getAfterPartyTicketsTotal());

            // Create seat tiers
            Map<String, Seat> seatTiers = new ConcurrentHashMap<>();
            for (SeatTier tier : request.getSeatTiersList()) {
                Seat seat = new Seat();
                seat.setTierName(tier.getName());
                seat.setPrice(tier.getPrice());
                seat.setTotalSeats(tier.getQuantity());
                seat.setAvailableSeats(tier.getQuantity());
                seatTiers.put(tier.getName(), seat);
            }
            concert.setSeatTiers(seatTiers);

            // Prepare data for two-phase commit
            Map<String, Object> data = new HashMap<>();
            data.put("operation", "ADD_CONCERT");
            data.put("concert", concert);

            // Initiate two-phase commit across all nodes
            boolean success = twoPhaseCommit.execute(data);

            AddConcertResponse response;
            if (success) {
                response = AddConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setConcertId(concertId)
                        .build();
            } else {
                response = AddConcertResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Failed to add concert across all nodes")
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error adding concert", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            distributedLock.releaseLock("concert");
        }
    }

    @Override
    public void updateConcert(UpdateConcertRequest request, StreamObserver<UpdateConcertResponse> responseObserver) {
        if (!isPrimary) {
            // Forward request to primary node if this is a secondary node
            forwardToPrimary(request, responseObserver);
            return;
        }

        try {
            // Acquire distributed lock to ensure consistency
            distributedLock.acquireLock("concert");

            // Get existing concert from repository
            Concert existingConcert = dataRepository.getConcert(request.getConcertId());
            if (existingConcert == null) {
                responseObserver.onNext(UpdateConcertResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Concert not found with ID: " + request.getConcertId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Update concert properties
            if (!request.getName().isEmpty()) {
                existingConcert.setName(request.getName());
            }
            if (request.getDate() > 0) {
                existingConcert.setDate(new Date(request.getDate()));
            }
            if (!request.getVenue().isEmpty()) {
                existingConcert.setVenue(request.getVenue());
            }

            // Update seat tiers if provided
            for (SeatTierUpdate tierUpdate : request.getSeatTierUpdatesList()) {
                Seat seat = existingConcert.getSeatTiers().get(tierUpdate.getName());
                if (seat != null) {
                    if (tierUpdate.hasPrice()) {
                        seat.setPrice(tierUpdate.getPrice());
                    }
                    if (tierUpdate.hasAvailability()) {
                        // Calculate the difference to update total seats
                        int diff = tierUpdate.getAvailability() - seat.getAvailableSeats();
                        seat.setTotalSeats(seat.getTotalSeats() + diff);
                        seat.setAvailableSeats(tierUpdate.getAvailability());
                    }
                }
            }

            // Update after-party tickets if provided
            if (request.hasAfterPartyTickets()) {
                int newAvailable = request.getAfterPartyTickets();
                int diff = newAvailable - existingConcert.getAfterPartyTicketsAvailable();
                existingConcert.setAfterPartyTicketsTotal(existingConcert.getAfterPartyTicketsTotal() + diff);
                existingConcert.setAfterPartyTicketsAvailable(newAvailable);
            }

            // Prepare data for two-phase commit
            Map<String, Object> data = new HashMap<>();
            data.put("operation", "UPDATE_CONCERT");
            data.put("concert", existingConcert);

            // Initiate two-phase commit across all nodes
            boolean success = twoPhaseCommit.execute(data);

            UpdateConcertResponse response;
            if (success) {
                response = UpdateConcertResponse.newBuilder()
                        .setSuccess(true)
                        .build();
            } else {
                response = UpdateConcertResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Failed to update concert across all nodes")
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error updating concert", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            distributedLock.releaseLock("concert");
        }
    }

    @Override
    public void cancelConcert(CancelConcertRequest request, StreamObserver<CancelConcertResponse> responseObserver) {
        if (!isPrimary) {
            // Forward request to primary node if this is a secondary node
            forwardToPrimary(request, responseObserver);
            return;
        }

        try {
            // Acquire distributed lock to ensure consistency
            distributedLock.acquireLock("concert");

            // Check if concert exists
            Concert existingConcert = dataRepository.getConcert(request.getConcertId());
            if (existingConcert == null) {
                responseObserver.onNext(CancelConcertResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Concert not found with ID: " + request.getConcertId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Prepare data for two-phase commit
            Map<String, Object> data = new HashMap<>();
            data.put("operation", "CANCEL_CONCERT");
            data.put("concertId", request.getConcertId());

            // Initiate two-phase commit across all nodes
            boolean success = twoPhaseCommit.execute(data);

            CancelConcertResponse response;
            if (success) {
                response = CancelConcertResponse.newBuilder()
                        .setSuccess(true)
                        .build();
            } else {
                response = CancelConcertResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Failed to cancel concert across all nodes")
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error canceling concert", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            distributedLock.releaseLock("concert");
        }
    }

    @Override
    public void getConcert(GetConcertRequest request, StreamObserver<GetConcertResponse> responseObserver) {
        try {
            // Get concert from repository
            Concert concert = dataRepository.getConcert(request.getConcertId());
            if (concert == null) {
                responseObserver.onNext(GetConcertResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Concert not found with ID: " + request.getConcertId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Build response
            GetConcertResponse.Builder responseBuilder = GetConcertResponse.newBuilder()
                    .setSuccess(true)
                    .setConcertId(concert.getId())
                    .setName(concert.getName())
                    .setDate(concert.getDate().getTime())
                    .setVenue(concert.getVenue())
                    .setAfterPartyTicketsAvailable(concert.getAfterPartyTicketsAvailable());

            // Add seat tiers to response
            for (Map.Entry<String, Seat> entry : concert.getSeatTiers().entrySet()) {
                Seat seat = entry.getValue();
                SeatTierInfo tierInfo = SeatTierInfo.newBuilder()
                        .setName(seat.getTierName())
                        .setPrice(seat.getPrice())
                        .setAvailableSeats(seat.getAvailableSeats())
                        .setTotalSeats(seat.getTotalSeats())
                        .build();
                responseBuilder.addSeatTiers(tierInfo);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error getting concert", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void listConcerts(ListConcertsRequest request, StreamObserver<ListConcertsResponse> responseObserver) {
        try {
            // Get all concerts from repository
            List<Concert> concerts = dataRepository.getAllConcerts();

            // Build response
            ListConcertsResponse.Builder responseBuilder = ListConcertsResponse.newBuilder();

            for (Concert concert : concerts) {
                ConcertSummary.Builder summaryBuilder = ConcertSummary.newBuilder()
                        .setConcertId(concert.getId())
                        .setName(concert.getName())
                        .setDate(concert.getDate().getTime())
                        .setVenue(concert.getVenue())
                        .setAfterPartyAvailable(concert.getAfterPartyTicketsAvailable() > 0);

                // Add availability info for each tier
                for (Map.Entry<String, Seat> entry : concert.getSeatTiers().entrySet()) {
                    Seat seat = entry.getValue();
                    summaryBuilder.addSeatAvailability(
                            SeatAvailability.newBuilder()
                                    .setTierName(seat.getTierName())
                                    .setAvailable(seat.getAvailableSeats())
                                    .setPrice(seat.getPrice())
                                    .build()
                    );
                }

                responseBuilder.addConcerts(summaryBuilder.build());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error listing concerts", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    // Helper method to forward requests to the primary node
    private <ReqT, RespT> void forwardToPrimary(ReqT request, StreamObserver<RespT> responseObserver) {
        // Implementation depends on how you're handling communication with the primary node
        // This could involve using a gRPC client to contact the primary node

        // For now, we'll just return an error
        responseObserver.onError(Status.FAILED_PRECONDITION
                .withDescription("This node is not the primary. Request forwarding not implemented yet.")
                .asRuntimeException());
    }

    // Method to apply changes during two-phase commit
    public boolean applyCommit(Map<String, Object> data) {
        try {
            String operation = (String) data.get("operation");

            switch (operation) {
                case "ADD_CONCERT":
                    Concert newConcert = (Concert) data.get("concert");
                    dataRepository.addConcert(newConcert);
                    break;

                case "UPDATE_CONCERT":
                    Concert updatedConcert = (Concert) data.get("concert");
                    dataRepository.updateConcert(updatedConcert);
                    break;

                case "CANCEL_CONCERT":
                    String concertId = (String) data.get("concertId");
                    dataRepository.deleteConcert(concertId);
                    break;

                default:
                    logger.warn("Unknown operation in commit: " + operation);
                    return false;
            }

            return true;
        } catch (Exception e) {
            logger.error("Error applying commit", e);
            return false;
        }
    }

    // Method to prepare for commit (validate data)
    public boolean prepareCommit(Map<String, Object> data) {
        try {
            String operation = (String) data.get("operation");

            switch (operation) {
                case "ADD_CONCERT":
                    Concert newConcert = (Concert) data.get("concert");
                    // Validate concert data
                    if (newConcert == null || newConcert.getName() == null || newConcert.getVenue() == null) {
                        return false;
                    }
                    break;

                case "UPDATE_CONCERT":
                    Concert updatedConcert = (Concert) data.get("concert");
                    // Validate concert exists
                    if (updatedConcert == null || dataRepository.getConcert(updatedConcert.getId()) == null) {
                        return false;
                    }
                    break;

                case "CANCEL_CONCERT":
                    String concertId = (String) data.get("concertId");
                    // Validate concert exists
                    if (concertId == null || dataRepository.getConcert(concertId) == null) {
                        return false;
                    }
                    break;

                default:
                    logger.warn("Unknown operation in prepare: " + operation);
                    return false;
            }

            return true;
        } catch (Exception e) {
            logger.error("Error preparing commit", e);
            return false;
        }
    }
}