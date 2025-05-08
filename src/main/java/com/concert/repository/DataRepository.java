package com.concert.repository;

import com.concert.model.Concert;
import com.concert.model.Reservation;
import com.concert.model.Seat;
import com.concert.model.AfterPartyTicket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.io.Serializable;

/**
 * Repository for storing and managing concert data in memory
 * Acts as a simple in-memory database for the distributed system
 */
public class DataRepository implements Serializable {
    private static final long serialVersionUID = 1L;

    // In-memory storage
    private final Map<String, Concert> concerts;
    private final Map<String, Reservation> reservations;

    // Singleton instance
    private static DataRepository instance;

    private DataRepository() {
        concerts = new ConcurrentHashMap<>();
        reservations = new ConcurrentHashMap<>();
    }

    public static synchronized DataRepository getInstance() {
        if (instance == null) {
            instance = new DataRepository();
        }
        return instance;
    }

    // For data sync between nodes
    public void syncData(DataRepository other) {
        this.concerts.clear();
        this.concerts.putAll(other.concerts);

        this.reservations.clear();
        this.reservations.putAll(other.reservations);
    }

    // Concert management
    public void saveConcert(Concert concert) {
        concerts.put(concert.getId(), concert);
    }

    public Optional<Concert> getConcertById(String id) {
        return Optional.ofNullable(concerts.get(id));
    }

    public List<Concert> getAllConcerts() {
        return concerts.values().stream()
                .filter(concert -> !concert.isCanceled())
                .collect(Collectors.toList());
    }

    public boolean cancelConcert(String id) {
        Concert concert = concerts.get(id);
        if (concert != null) {
            concert.setCanceled(true);
            return true;
        }
        return false;
    }

    public boolean deleteConcert(String id) {
        return concerts.remove(id) != null;
    }

    // Reservation management
    public void saveReservation(Reservation reservation) {
        reservations.put(reservation.getId(), reservation);
    }

    public Optional<Reservation> getReservationById(String id) {
        return Optional.ofNullable(reservations.get(id));
    }

    public List<Reservation> getReservationsByConcertId(String concertId) {
        return reservations.values().stream()
                .filter(r -> r.getConcertId().equals(concertId))
                .collect(Collectors.toList());
    }

    public boolean deleteReservation(String id) {
        return reservations.remove(id) != null;
    }

    // Ticket availability management
    public synchronized boolean checkAndReserveSeats(String concertId, String seatTier, int quantity) {
        Concert concert = concerts.get(concertId);
        if (concert == null || concert.isCanceled()) {
            return false;
        }

        Seat seat = concert.getSeatTiers().get(seatTier);
        if (seat == null || seat.getAvailableQuantity() < quantity) {
            return false;
        }

        // Reserve seats by reducing availability
        seat.setAvailableQuantity(seat.getAvailableQuantity() - quantity);
        return true;
    }

    public synchronized boolean checkAndReserveAfterPartyTickets(String concertId, int quantity) {
        Concert concert = concerts.get(concertId);
        if (concert == null || concert.isCanceled()) {
            return false;
        }

        if (concert.getAfterPartyTicketsAvailable() < quantity) {
            return false;
        }

        // Reserve after-party tickets
        concert.setAfterPartyTicketsAvailable(concert.getAfterPartyTicketsAvailable() - quantity);
        return true;
    }

    // For rolling back failed transactions
    public synchronized void releaseSeats(String concertId, String seatTier, int quantity) {
        Concert concert = concerts.get(concertId);
        if (concert != null) {
            Seat seat = concert.getSeatTiers().get(seatTier);
            if (seat != null) {
                seat.setAvailableQuantity(seat.getAvailableQuantity() + quantity);
            }
        }
    }

    public synchronized void releaseAfterPartyTickets(String concertId, int quantity) {
        Concert concert = concerts.get(concertId);
        if (concert != null) {
            concert.setAfterPartyTicketsAvailable(concert.getAfterPartyTicketsAvailable() + quantity);
        }
    }

    // For state transfer during node recovery
    public Map<String, Concert> getConcertsSnapshot() {
        return new ConcurrentHashMap<>(concerts);
    }

    public Map<String, Reservation> getReservationsSnapshot() {
        return new ConcurrentHashMap<>(reservations);
    }

    public void loadFromSnapshot(Map<String, Concert> concertsData, Map<String, Reservation> reservationsData) {
        this.concerts.clear();
        this.concerts.putAll(concertsData);

        this.reservations.clear();
        this.reservations.putAll(reservationsData);
    }
}