package com.concert.server;

import com.concert.coordination.LeaderElection;
import com.concert.coordination.ZookeeperCoordinator;
import com.concert.model.AfterPartyTicket;
import com.concert.model.Concert;
import com.concert.model.Reservation;
import com.concert.model.Seat;
import com.concert.nameservice.EtcdNameService;
import com.concert.repository.DataRepository;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for the Server components of the Concert Ticket Reservation System
 */
public class ServerTests {

    @Mock
    private ZookeeperCoordinator mockZookeeperCoordinator;

    @Mock
    private EtcdNameService mockNameService;

    @Mock
    private LeaderElection mockLeaderElection;

    @Mock
    private StreamObserver<Boolean> mockResponseObserver;

    private ServerNode serverNode;
    private DataRepository dataRepository;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        // Create a real data repository for testing
        dataRepository = new DataRepository();

        // Create server node with mocked dependencies
        serverNode = new ServerNode(8080, dataRepository, mockZookeeperCoordinator, mockNameService, mockLeaderElection);

        // Make server node primary for testing
        when(mockLeaderElection.isLeader()).thenReturn(true);
        serverNode.handleLeadershipChange(true);
    }

    @Test
    public void testAddConcert() {
        // Create a concert for testing
        Concert concert = createTestConcert();

        // Call add concert method
        serverNode.addConcert(concert, mockResponseObserver);

        // Verify response observer was called
        verify(mockResponseObserver).onNext(any(Boolean.class));
        verify(mockResponseObserver).onCompleted();

        // Verify concert was added to repository
        Concert storedConcert = dataRepository.getConcert(concert.getId());
        assertNotNull(storedConcert);
        assertEquals(concert.getName(), storedConcert.getName());
    }

    @Test
    public void testUpdateConcert() {
        // Create and add a concert
        Concert concert = createTestConcert();
        dataRepository.addConcert(concert);

        // Update concert
        concert.setName("Updated Concert Name");
        serverNode.updateConcert(concert, mockResponseObserver);

        // Verify response observer was called
        verify(mockResponseObserver).onNext(any(Boolean.class));
        verify(mockResponseObserver).onCompleted();

        // Verify concert was updated in repository
        Concert updatedConcert = dataRepository.getConcert(concert.getId());
        assertNotNull(updatedConcert);
        assertEquals("Updated Concert Name", updatedConcert.getName());
    }

    @Test
    public void testCancelConcert() {
        // Create and add a concert
        Concert concert = createTestConcert();
        dataRepository.addConcert(concert);

        // Cancel concert
        serverNode.cancelConcert(concert.getId(), mockResponseObserver);

        // Verify response observer was called
        verify(mockResponseObserver).onNext(any(Boolean.class));
        verify(mockResponseObserver).onCompleted();

        // Verify concert was removed from repository
        Concert cancelledConcert = dataRepository.getConcert(concert.getId());
        assertNull(cancelledConcert);
    }

    @Test
    public void testUpdateTicketStock() {
        // Create and add a concert
        Concert concert = createTestConcert();
        dataRepository.addConcert(concert);

        // Update ticket stock
        String seatType = "VIP";
        int additionalQuantity = 20;
        serverNode.updateTicketStock(concert.getId(), seatType, additionalQuantity, mockResponseObserver);

        // Verify response observer was called
        verify(mockResponseObserver).onNext(any(Boolean.class));
        verify(mockResponseObserver).onCompleted();

        // Verify ticket stock was updated in repository
        Concert updatedConcert = dataRepository.getConcert(concert.getId());
        assertNotNull(updatedConcert);

        // Find VIP seat and check quantity
        for (Seat seat : updatedConcert.getSeats()) {
            if (seat.getType().equals(seatType)) {
                assertEquals(120, seat.getQuantity()); // Original 100 + 20 additional
                break;
            }
        }
    }

    @Test
    public void testMakeReservation() {
        // Create and add a concert
        Concert concert = createTestConcert();
        dataRepository.addConcert(concert);

        // Create a reservation
        Reservation reservation = createTestReservation(concert.getId());

        // Make reservation
        serverNode.makeReservation(reservation, mockResponseObserver);

        // Verify response observer was called
        verify(mockResponseObserver).onNext(any(Boolean.class));
        verify(mockResponseObserver).onCompleted();

        // Verify reservation was added to repository
        Reservation storedReservation = dataRepository.getReservation(reservation.getId());
        assertNotNull(storedReservation);
        assertEquals(reservation.getCustomerId(), storedReservation.getCustomerId());

        // Verify ticket quantities were updated
        Concert updatedConcert = dataRepository.getConcert(concert.getId());
        assertNotNull(updatedConcert);

        // Find Regular seat and check quantity
        for (Seat seat : updatedConcert.getSeats()) {
            if (seat.getType().equals("Regular")) {
                assertEquals(198, seat.getQuantity()); // Original 200 - 2 reserved
                break;
            }
        }

        // Verify after-party tickets were updated
        assertEquals(99, updatedConcert.getAfterPartyTickets().getQuantity()); // Original 100 - 1 reserved
    }

    @Test
    public void testForwardRequestWhenNotPrimary() {
        // Make node secondary
        serverNode.handleLeadershipChange(false);

        // Create a concert
        Concert concert = createTestConcert();

        // Call add concert method
        serverNode.addConcert(concert, mockResponseObserver);

        // Verify response observer was called
        verify(mockResponseObserver).onNext(eq(false)); // Should fail because forwarding is not fully implemented in test
        verify(mockResponseObserver).onCompleted();

        // Verify concert was not added to repository
        Concert storedConcert = dataRepository.getConcert(concert.getId());
        assertNull(storedConcert);
    }

    @Test
    public void testConcurrentReservations() {
        // Create and add a concert with limited seats
        Concert concert = createTestConcert();
        // Set Regular seats to only 2
        for (Seat seat : concert.getSeats()) {
            if (seat.getType().equals("Regular")) {
                seat.setQuantity(2);
            }
        }
        dataRepository.addConcert(concert);

        // Create two reservations for the same seat type
        Reservation reservation1 = createTestReservation(concert.getId());
        Reservation reservation2 = createTestReservation(concert.getId());

        // Make first reservation
        serverNode.makeReservation(reservation1, mockResponseObserver);

        // Verify first reservation was successful
        verify(mockResponseObserver).onNext(eq(true));
        verify(mockResponseObserver).onCompleted();

        // Reset mock for second reservation
        reset(mockResponseObserver);

        // Make second reservation
        serverNode.makeReservation(reservation2, mockResponseObserver);

        // Verify second reservation was also successful (we still have enough seats)
        verify(mockResponseObserver).onNext(eq(true));
        verify(mockResponseObserver).onCompleted();

        // Verify seat quantity was updated correctly
        Concert updatedConcert = dataRepository.getConcert(concert.getId());
        for (Seat seat : updatedConcert.getSeats()) {
            if (seat.getType().equals("Regular")) {
                assertEquals(0, seat.getQuantity()); // All seats reserved
                break;
            }
        }

        // Reset mock for third reservation
        reset(mockResponseObserver);

        // Create a third reservation that should fail due to no more seats
        Reservation reservation3 = createTestReservation(concert.getId());
        serverNode.makeReservation(reservation3, mockResponseObserver);

        // Verify third reservation failed
        verify(mockResponseObserver).onNext(eq(false));
        verify(mockResponseObserver).onCompleted();
    }

    /**
     * Helper method to create a test concert
     */
    private Concert createTestConcert() {
        String concertId = UUID.randomUUID().toString();
        Concert concert = new Concert(concertId, "Test Concert", "2025-06-01", "Test Venue");

        // Add seats
        concert.addSeat(new Seat("Regular", 200, 50.0));
        concert.addSeat(new Seat("VIP", 100, 100.0));

        // Add after-party tickets
        concert.setAfterPartyTickets(new AfterPartyTicket(100, 30.0));

        return concert;
    }

    /**
     * Helper method to create a test reservation
     */
    private Reservation createTestReservation(String concertId) {
        String reservationId = UUID.randomUUID().toString();
        String customerId = "customer-" + UUID.randomUUID().toString();

        Reservation reservation = new Reservation(reservationId, customerId, concertId);

        // Add seats to reservation
        Map<String, Integer> seats = new HashMap<>();
        seats.put("Regular", 2); // Reserve 2 regular seats
        reservation.setSeats(seats);

        // Add after-party ticket
        reservation.setIncludeAfterParty(true);
        reservation.setAfterPartyTickets(1);

        return reservation;
    }
}