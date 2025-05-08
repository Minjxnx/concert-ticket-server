package com.concert.model;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents an after-party ticket in the system.
 */
public class AfterPartyTicket implements Serializable {
    private String id;
    private String concertId;
    private String reservationId;
    private String customerName;
    private double price;
    private boolean isUsed;
    private long creationTimestamp;

    public AfterPartyTicket() {
        this.id = UUID.randomUUID().toString();
        this.isUsed = false;
        this.creationTimestamp = System.currentTimeMillis();
    }

    public AfterPartyTicket(String concertId, String reservationId, String customerName, double price) {
        this();
        this.concertId = concertId;
        this.reservationId = reservationId;
        this.customerName = customerName;
        this.price = price;
    }

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getConcertId() {
        return concertId;
    }

    public void setConcertId(String concertId) {
        this.concertId = concertId;
    }

    public String getReservationId() {
        return reservationId;
    }

    public void setReservationId(String reservationId) {
        this.reservationId = reservationId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public boolean isUsed() {
        return isUsed;
    }

    public void setUsed(boolean used) {
        isUsed = used;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    @Override
    public String toString() {
        return "AfterPartyTicket{" +
                "id='" + id + '\'' +
                ", concertId='" + concertId + '\'' +
                ", reservationId='" + reservationId + '\'' +
                ", customerName='" + customerName + '\'' +
                ", price=" + price +
                ", isUsed=" + isUsed +
                '}';
    }
}