package com.concert.service;

import com.concert.consensus.TwoPhaseCommit;
import com.concert.coordination.DistributedLock;
import com.concert.model.Concert;
import com.concert.model.Seat;
import com.concert.repository.DataRepository;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Service implementation for managing ticket inventory in the distributed system.
 * Handles operations for updating ticket availability and pricing.
 */
public class TicketInventoryService extends TicketInventoryServiceGrpc.TicketInventoryServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(TicketInventoryService.class);

    private final DataRepository dataRepository;
    private final TwoPhaseCommit twoPhaseCommit;
    private final DistributedLock distributedLock;
    private final boolean isPrimary;

    public TicketInventoryService(DataRepository dataRepository, TwoPhaseCommit twoPhaseCommit,
                                  DistributedLock distributedLock, boolean isPrimary) {
        this.dataRepository = dataRepository;
        this.twoPhaseCommit = twoPhaseCommit;
        this.distributedLock = distributedLock;
        this.isPrimary = isPrimary;
    }

    @Override
    public void updateTicketStock(UpdateTicketStockRequest request,
                                  StreamObserver<UpdateTicketStockResponse> responseObserver) {
        if (!isPrimary) {
            // Forward request to primary node if this is a secondary node
            forwardToPrimary(request, responseObserver);
            return;
        }

        try {
            // Acquire distributed lock to ensure consistency
            distributedLock.acquireLock("ticket");

            // Get concert from repository
            Concert concert = dataRepository.getConcert(request.getConcertId());
            if (concert == null) {
                responseObserver.onNext(UpdateTicketStockResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Concert not found with ID: " + request.getConcertId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check ticket type
            if (request.getTicketType().equalsIgnoreCase("after-party")) {
                // Handle after-party ticket update
                int newTotal = concert.getAfterPartyTicketsTotal() + request.getQuantityChange();
                int newAvailable = concert.getAfterPartyTicketsAvailable() + request.getQuantityChange();

                if (newTotal < 0 || newAvailable < 0) {
                    responseObserver.onNext(UpdateTicketStockResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("Cannot reduce after-party tickets below zero")
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                // Prepare data for two-phase commit
                Map<String, Object> data = new HashMap<>();
                data.put("operation", "UPDATE_AFTER_PARTY_TICKETS");
                data.put("concertId", request.getConcertId());
                data.put("newTotal", newTotal);
                data.put("newAvailable", newAvailable);

                // Initiate two-phase commit across all nodes
                boolean success = twoPhaseCommit.execute(data);

                UpdateTicketStockResponse response;
                if (success) {
                    response = UpdateTicketStockResponse.newBuilder()
                            .setSuccess(true)
                            .setNewQuantity(newAvailable)
                            .build();
                } else {
                    response = UpdateTicketStockResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("Failed to update after-party tickets across all nodes")
                            .build();
                }

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                // Handle regular seat tier update
                Seat seat = concert.getSeatTiers().get(request.getTicketType());
                if (seat == null) {
                    responseObserver.onNext(UpdateTicketStockResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("Seat tier not found: " + request.getTicketType())
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                int newTotal = seat.getTotalSeats() + request.getQuantityChange();
                int newAvailable = seat.getAvailableSeats() + request.getQuantityChange();

                if (newTotal < 0 || newAvailable < 0) {
                    responseObserver.onNext(UpdateTicketStockResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("Cannot reduce seats below zero")
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                // Prepare data for two-phase commit
                Map<String, Object> data = new HashMap<>();
                data.put("operation", "UPDATE_SEAT_TIER");
                data.put("concertId", request.getConcertId());
                data.put("tierName", request.getTicketType());
                data.put("newTotal", newTotal);
                data.put("newAvailable", newAvailable);

                // Initiate two-phase commit across all nodes
                boolean success = twoPhaseCommit.execute(data);

                UpdateTicketStockResponse response;
                if (success) {
                    response = UpdateTicketStockResponse.newBuilder()
                            .setSuccess(true)
                            .setNewQuantity(newAvailable)
                            .build();
                } else {
                    response = UpdateTicketStockResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("Failed to update seat tier across all nodes")
                            .build();
                }

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        } catch (Exception e) {
            logger.error("Error updating ticket stock", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            distributedLock.releaseLock("ticket");
        }
    }

    @Override
    public void updateTicketPrice(UpdateTicketPriceRequest request,
                                  StreamObserver<UpdateTicketPriceResponse> responseObserver) {
        if (!isPrimary) {
            // Forward request to primary node if this is a secondary node
            forwardToPrimary(request, responseObserver);
            return;
        }

        try {
            // Acquire distributed lock to ensure consistency
            distributedLock.acquireLock("ticket");

            // Get concert from repository
            Concert concert = dataRepository.getConcert(request.getConcertId());
            if (concert == null) {
                responseObserver.onNext(UpdateTicketPriceResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Concert not found with ID: " + request.getConcertId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check seat tier exists
            Seat seat = concert.getSeatTiers().get(request.getSeatTier());
            if (seat == null) {
                responseObserver.onNext(UpdateTicketPriceResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Seat tier not found: " + request.getSeatTier())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Validate new price
            if (request.getNewPrice() <= 0) {
                responseObserver.onNext(UpdateTicketPriceResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Price must be greater than zero")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Prepare data for two-phase commit
            Map<String, Object> data = new HashMap<>();
            data.put("operation", "UPDATE_TICKET_PRICE");
            data.put("concertId", request.getConcertId());
            data.put("tierName", request.getSeatTier());
            data.put("newPrice", request.getNewPrice());

            // Initiate two-phase commit across all nodes
            boolean success = twoPhaseCommit.execute(data);

            UpdateTicketPriceResponse response;
            if (success) {
                response = UpdateTicketPriceResponse.newBuilder()
                        .setSuccess(true)
                        .build();
            } else {
                response = UpdateTicketPriceResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Failed to update ticket price across all nodes")
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error updating ticket price", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            distributedLock.releaseLock("ticket");
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
            String concertId = (String) data.get("concertId");

            // Get concert from repository
            Concert concert = dataRepository.getConcert(concertId);
            if (concert == null) {
                return false;
            }

            switch (operation) {
                case "UPDATE_AFTER_PARTY_TICKETS":
                    int newAfterPartyTotal = (int) data.get("newTotal");
                    int newAfterPartyAvailable = (int) data.get("newAvailable");

                    concert.setAfterPartyTicketsTotal(newAfterPartyTotal);
                    concert.setAfterPartyTicketsAvailable(newAfterPartyAvailable);
                    break;

                case "UPDATE_SEAT_TIER":
                    String tierName = (String) data.get("tierName");
                    int newTotal = (int) data.get("newTotal");
                    int newAvailable = (int) data.get("newAvailable");

                    Seat seat = concert.getSeatTiers().get(tierName);
                    if (seat == null) {
                        return false;
                    }

                    seat.setTotalSeats(newTotal);
                    seat.setAvailableSeats(newAvailable);
                    break;

                case "UPDATE_TICKET_PRICE":
                    String seatTier = (String) data.get("tierName");
                    double newPrice = (double) data.get("newPrice");

                    Seat seatToUpdate = concert.getSeatTiers().get(seatTier);
                    if (seatToUpdate == null) {
                        return false;
                    }

                    seatToUpdate.setPrice(newPrice);
                    break;

                default:
                    logger.warn("Unknown operation in commit: " + operation);
                    return false;
            }

            // Update the concert in repository
            dataRepository.updateConcert(concert);
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
            String concertId = (String) data.get("concertId");

            // Get concert from repository
            Concert concert = dataRepository.getConcert(concertId);
            if (concert == null) {
                return false;
            }

            switch (operation) {
                case "UPDATE_AFTER_PARTY_TICKETS":
                    int newAfterPartyTotal = (int) data.get("newTotal");
                    int newAfterPartyAvailable = (int) data.get("newAvailable");

                    // Validate new values
                    if (newAfterPartyTotal < 0 || newAfterPartyAvailable < 0 ||
                            newAfterPartyAvailable > newAfterPartyTotal) {
                        return false;
                    }
                    break;

                case "UPDATE_SEAT_TIER":
                    String tierName = (String) data.get("tierName");
                    int newTotal = (int) data.get("newTotal");
                    int newAvailable = (int) data.get("newAvailable");

                    // Validate seat tier exists
                    Seat seat = concert.getSeatTiers().get(tierName);
                    if (seat == null) {
                        return false;
                    }

                    // Validate new values
                    if (newTotal < 0 || newAvailable < 0 || newAvailable > newTotal) {
                        return false;
                    }
                    break;

                case "UPDATE_TICKET_PRICE":
                    String seatTier = (String) data.get("tierName");
                    double newPrice = (double) data.get("newPrice");

                    // Validate seat tier exists
                    Seat seatToUpdate = concert.getSeatTiers().get(seatTier);
                    if (seatToUpdate == null) {
                        return false;
                    }

                    // Validate new price
                    if (newPrice <= 0) {
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