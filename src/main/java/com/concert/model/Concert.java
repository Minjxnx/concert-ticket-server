package com.concert.model;

import com.concert.grpc.SeatTier;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents a concert event in the system.
 */
public class Concert implements Serializable {
    private String id;
    private String name;
    private String date;
    private String venue;
    private String description;
    private boolean hasAfterParty;
    private int afterPartyTickets;
    private List<Seat> seatTiers;
    private boolean isCancelled;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;

    public Concert() {
        this.id = UUID.randomUUID().toString();
        this.seatTiers = new ArrayList<>();
        this.isCancelled = false;
        this.creationTimestamp = System.currentTimeMillis();
        this.lastUpdatedTimestamp = this.creationTimestamp;
    }

    public Concert(String name, String date, String venue, String description, boolean hasAfterParty, int afterPartyTickets) {
        this();
        this.name = name;
        this.date = date;
        this.venue = venue;
        this.description = description;
        this.hasAfterParty = hasAfterParty;
        this.afterPartyTickets = afterPartyTickets;
    }

    // Convert from gRPC Concert to domain Concert
    public static Concert fromGrpcConcert(com.concert.grpc.Concert grpcConcert) {
        Concert concert = new Concert();
        concert.setId(grpcConcert.getId());
        concert.setName(grpcConcert.getName());
        concert.setDate(grpcConcert.getDate());
        concert.setVenue(grpcConcert.getVenue());
        concert.setDescription(grpcConcert.getDescription());
        concert.setHasAfterParty(grpcConcert.getHasAfterParty());
        concert.setAfterPartyTickets(grpcConcert.getAfterPartyTickets());
        concert.setCancelled(grpcConcert.getIsCancelled());

        // Convert seat tiers
        for (SeatTier grpcSeatTier : grpcConcert.getSeatTiersList()) {
            Seat seat = new Seat(
                    grpcSeatTier.getName(),
                    grpcSeatTier.getPrice(),
                    grpcSeatTier.getTotalSeats(),
                    grpcSeatTier.getAvailableSeats()
            );
            concert.addSeatTier(seat);
        }

        return concert;
    }

    // Convert to gRPC Concert
    public com.concert.grpc.Concert toGrpcConcert() {
        com.concert.grpc.Concert.Builder builder = com.concert.grpc.Concert.newBuilder()
                .setId(this.id)
                .setName(this.name)
                .setDate(this.date)
                .setVenue(this.venue)
                .setDescription(this.description)
                .setHasAfterParty(this.hasAfterParty)
                .setAfterPartyTickets(this.afterPartyTickets)
                .setIsCancelled(this.isCancelled);

        // Add seat tiers
        for (Seat seat : this.seatTiers) {
            SeatTier seatTier = SeatTier.newBuilder()
                    .setName(seat.getName())
                    .setPrice(seat.getPrice())
                    .setTotalSeats(seat.getTotalSeats())
                    .setAvailableSeats(seat.getAvailableSeats())
                    .build();
            builder.addSeatTiers(seatTier);
        }

        return builder.build();
    }

    public synchronized boolean reserveSeats(String tierName, int quantity) {
        for (Seat seat : seatTiers) {
            if (seat.getName().equals(tierName)) {
                if (seat.getAvailableSeats() >= quantity) {
                    seat.setAvailableSeats(seat.getAvailableSeats() - quantity);
                    this.lastUpdatedTimestamp = System.currentTimeMillis();
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    public synchronized boolean releaseSeats(String tierName, int quantity) {
        for (Seat seat : seatTiers) {
            if (seat.getName().equals(tierName)) {
                seat.setAvailableSeats(seat.getAvailableSeats() + quantity);
                this.lastUpdatedTimestamp = System.currentTimeMillis();
                return true;
            }
        }
        return false;
    }

    public synchronized boolean reserveAfterPartyTickets(int quantity) {
        if (hasAfterParty && afterPartyTickets >= quantity) {
            afterPartyTickets -= quantity;
            this.lastUpdatedTimestamp = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public synchronized boolean releaseAfterPartyTickets(int quantity) {
        if (hasAfterParty) {
            afterPartyTickets += quantity;
            this.lastUpdatedTimestamp = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public void addSeatTier(Seat seat) {
        this.seatTiers.add(seat);
        this.lastUpdatedTimestamp = System.currentTimeMillis();
    }

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getVenue() {
        return venue;
    }

    public void setVenue(String venue) {
        this.venue = venue;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isHasAfterParty() {
        return hasAfterParty;
    }

    public void setHasAfterParty(boolean hasAfterParty) {
        this.hasAfterParty = hasAfterParty;
    }

    public int getAfterPartyTickets() {
        return afterPartyTickets;
    }

    public void setAfterPartyTickets(int afterPartyTickets) {
        this.afterPartyTickets = afterPartyTickets;
    }

    public List<Seat> getSeatTiers() {
        return seatTiers;
    }

    public void setSeatTiers(List<Seat> seatTiers) {
        this.seatTiers = seatTiers;
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public void setCancelled(boolean cancelled) {
        isCancelled = cancelled;
        this.lastUpdatedTimestamp = System.currentTimeMillis();
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public long getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(long lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    @Override
    public String toString() {
        return "Concert{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", date='" + date + '\'' +
                ", venue='" + venue + '\'' +
                ", hasAfterParty=" + hasAfterParty +
                ", afterPartyTickets=" + afterPartyTickets +
                ", isCancelled=" + isCancelled +
                '}';
    }
}