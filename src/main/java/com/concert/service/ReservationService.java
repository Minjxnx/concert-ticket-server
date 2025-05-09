
package com.concert.service;

import com.concert.consensus.TwoPhaseCommit;
import com.concert.coordination.DistributedLock;
import com.concert.model.Concert;
import com.concert.model.Reservation;
import com.concert.model.Seat;
import com.concert.repository.DataRepository;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Service implementation for handling reservations in the distributed system.
 * Manages making reservations for concerts and optional after-party tickets.
 */
public class ReservationService extends ReservationServiceGrpc.ReservationServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ReservationService.class);

    private final DataRepository dataRepository;
    private final TwoPhaseCommit twoPhaseCommit;
    private final DistributedLock distributedLock;
    private final boolean isPrimary;

    public ReservationService(DataRepository dataRepository, TwoPhaseCommit twoPhaseCommit,
                              DistributedLock distributedLock, boolean isPrimary) {
        this.dataRepository = dataRepository;
        this.twoPhaseCommit = twoPhaseCommit;
        this.distributedLock = distributedLock;
        this.isPrimary = isPrimary;
    }

    @Override
    public void makeReservation(MakeReservationRequest request,
                                StreamObserver<MakeReservationResponse> responseObserver) {
        if (!isPrimary) {
            // Forward request to primary node if this is a secondary node
            forwardToPrimary(request, responseObserver);
            return;
        }

        try {
            // Acquire distributed lock to ensure consistency
            distributedLock.acquireLock("reservation");

            // Get concert from repository
            Concert concert = dataRepository.getConcert(request.getConcertId());
            if (concert == null) {
                responseObserver.onNext(MakeReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Concert not found with ID: " + request.getConcertId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check if requested seat tier exists
            Seat seatTier = concert.getSeatTiers().get(request.getSeatTier());
            if (seatTier == null) {
                responseObserver.onNext(MakeReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Seat tier not found: " + request.getSeatTier())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check seat availability
            if (seatTier.getAvailableSeats() < request.getNumberOfSeats()) {
                responseObserver.onNext(MakeReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Not enough seats available in tier: " + request.getSeatTier())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check after-party ticket availability if requested
            if (request.getIncludeAfterParty() &&
                    concert.getAfterPartyTicketsAvailable() < request.getNumberOfSeats()) {
                responseObserver.onNext(MakeReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Not enough after-party tickets available")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Create reservation object
            String reservationId = UUID.randomUUID().toString();
            Reservation reservation = new Reservation();
            reservation.setId(reservationId);
            reservation.setConcertId(request.getConcertId());
            reservation.setCustomerName(request.getCustomerName());
            reservation.setCustomerEmail(request.getCustomerEmail());
            reservation.setSeatTier(request.getSeatTier());
            reservation.setNumberOfSeats(request.getNumberOfSeats());
            reservation.setIncludeAfterParty(request.getIncludeAfterParty());
            reservation.setTotalPrice(calculateTotalPrice(seatTier, request.getNumberOfSeats(),
                    request.getIncludeAfterParty()));
            reservation.setReservationTime(new Date());

            // Prepare data for two-phase commit
            Map<String, Object> data = new HashMap<>();
            data.put("operation", "MAKE_RESERVATION");
            data.put("reservation", reservation);
            data.put("concertId", request.getConcertId());
            data.put("seatTier", request.getSeatTier());
            data.put("numberOfSeats", request.getNumberOfSeats());
            data.put("includeAfterParty", request.getIncludeAfterParty());

            // Initiate two-phase commit across all nodes
            boolean success = twoPhaseCommit.execute(data);

            MakeReservationResponse response;
            if (success) {
                response = MakeReservationResponse.newBuilder()
                        .setSuccess(true)
                        .setReservationId(reservationId)
                        .setTotalPrice(reservation.getTotalPrice())
                        .build();
            } else {
                response = MakeReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Failed to make reservation across all nodes")
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error making reservation", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            distributedLock.releaseLock("reservation");
        }
    }

    @Override
    public void bulkReservation(BulkReservationRequest request,
                                StreamObserver<BulkReservationResponse> responseObserver) {
        if (!isPrimary) {
            // Forward request to primary node if this is a secondary node
            forwardToPrimary(request, responseObserver);
            return;
        }

        try {
            // Acquire distributed lock to ensure consistency
            distributedLock.acquireLock("reservation");

            // Get concert from repository
            Concert concert = dataRepository.getConcert(request.getConcertId());
            if (concert == null) {
                responseObserver.onNext(BulkReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Concert not found with ID: " + request.getConcertId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check if requested seat tier exists
            Seat seatTier = concert.getSeatTiers().get(request.getSeatTier());
            if (seatTier == null) {
                responseObserver.onNext(BulkReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Seat tier not found: " + request.getSeatTier())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check seat availability
            if (seatTier.getAvailableSeats() < request.getNumberOfSeats()) {
                responseObserver.onNext(BulkReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Not enough seats available in tier: " + request.getSeatTier())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Check after-party ticket availability
            // For bulk reservations, after-party tickets are mandatory
            if (concert.getAfterPartyTicketsAvailable() < request.getNumberOfSeats()) {
                responseObserver.onNext(BulkReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Not enough after-party tickets available")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Create reservation object
            String reservationId = UUID.randomUUID().toString();
            Reservation reservation = new Reservation();
            reservation.setId(reservationId);
            reservation.setConcertId(request.getConcertId());
            reservation.setCustomerName(request.getGroupName());
            reservation.setCustomerEmail(request.getCoordinatorEmail());
            reservation.setSeatTier(request.getSeatTier());
            reservation.setNumberOfSeats(request.getNumberOfSeats());
            reservation.setIncludeAfterParty(true); // Bulk reservations always include after-party
            reservation.setGroupReservation(true);
            reservation.setTotalPrice(calculateTotalPrice(seatTier, request.getNumberOfSeats(), true));
            reservation.setReservationTime(new Date());

            // Prepare data for two-phase commit
            Map<String, Object> data = new HashMap<>();
            data.put("operation", "BULK_RESERVATION");
            data.put("reservation", reservation);
            data.put("concertId", request.getConcertId());
            data.put("seatTier", request.getSeatTier());
            data.put("numberOfSeats", request.getNumberOfSeats());
            data.put("includeAfterParty", true);

            // Initiate two-phase commit across all nodes
            boolean success = twoPhaseCommit.execute(data);

            BulkReservationResponse response;
            if (success) {
                response = BulkReservationResponse.newBuilder()
                        .setSuccess(true)
                        .setReservationId(reservationId)
                        .setTotalPrice(reservation.getTotalPrice())
                        .build();
            } else {
                response = BulkReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Failed to make bulk reservation across all nodes")
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error making bulk reservation", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            distributedLock.releaseLock("reservation");
        }
    }

    @Override
    public void getReservation(GetReservationRequest request,
                               StreamObserver<GetReservationResponse> responseObserver) {
        try {
            // Get reservation from repository
            Reservation reservation = dataRepository.getReservation(request.getReservationId());
            if (reservation == null) {
                responseObserver.onNext(GetReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Reservation not found with ID: " + request.getReservationId())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Get concert details
            Concert concert = dataRepository.getConcert(reservation.getConcertId());
            if (concert == null) {
                responseObserver.onNext(GetReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Associated concert not found")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Build response
            GetReservationResponse response = GetReservationResponse.newBuilder()
                    .setSuccess(true)
                    .setReservationId(reservation.getId())
                    .setConcertId(reservation.getConcertId())
                    .setConcertName(concert.getName())
                    .setCustomerName(reservation.isGroupReservation() ?
                            "Group: " + reservation.getCustomerName() :
                            reservation.getCustomerName())
                    .setCustomerEmail(reservation.getCustomerEmail())
                    .setSeatTier(reservation.getSeatTier())
                    .setNumberOfSeats(reservation.getNumberOfSeats())
                    .setIncludeAfterParty(reservation.isIncludeAfterParty())
                    .setTotalPrice(reservation.getTotalPrice())
                    .setReservationTime(reservation.getReservationTime().getTime())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error getting reservation", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    // Helper method to calculate total price
    private double calculateTotalPrice(Seat seatTier, int numberOfSeats, boolean includeAfterParty) {
        double totalPrice = seatTier.getPrice() * numberOfSeats;

        // Add price for after-party tickets (assuming a fixed price for now)
        if (includeAfterParty) {
            totalPrice += 50.0 * numberOfSeats; // $50 per after-party ticket
        }

        return totalPrice;
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
                case "MAKE_RESERVATION":
                case "BULK_RESERVATION":
                    // Add reservation to repository
                    Reservation reservation = (Reservation) data.get("reservation");
                    String concertId = (String) data.get("concertId");
                    String seatTier = (String) data.get("seatTier");
                    int numberOfSeats = (int) data.get("numberOfSeats");
                    boolean includeAfterParty = (boolean) data.get("includeAfterParty");

                    // Update concert seat availability
                    Concert concert = dataRepository.getConcert(concertId);
                    if (concert != null) {
                        Seat seat = concert.getSeatTiers().get(seatTier);
                        if (seat != null) {
                            seat.setAvailableSeats(seat.getAvailableSeats() - numberOfSeats);
                        }

                        // Update after-party ticket availability if needed
                        if (includeAfterParty) {
                            concert.setAfterPartyTicketsAvailable(
                                    concert.getAfterPartyTicketsAvailable() - numberOfSeats);
                        }

                        // Update the concert in repository
                        dataRepository.updateConcert(concert);
                    }

                    // Add the reservation
                    dataRepository.addReservation(reservation);
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
                case "MAKE_RESERVATION":
                case "BULK_RESERVATION":
                    String concertId = (String) data.get("concertId");
                    String seatTier = (String) data.get("seatTier");
                    int numberOfSeats = (int) data.get("numberOfSeats");
                    boolean includeAfterParty = (boolean) data.get("includeAfterParty");

                    // Validate concert exists
                    Concert concert = dataRepository.getConcert(concertId);
                    if (concert == null) {
                        return false;
                    }

                    // Validate seat tier exists
                    Seat seat = concert.getSeatTiers().get(seatTier);
                    if (seat == null) {
                        return false;
                    }

                    // Check seat availability
                    if (seat.getAvailableSeats() < numberOfSeats) {
                        return false;
                    }

                    // Check after-party ticket availability if needed
                    if (includeAfterParty && concert.getAfterPartyTicketsAvailable() < numberOfSeats) {
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