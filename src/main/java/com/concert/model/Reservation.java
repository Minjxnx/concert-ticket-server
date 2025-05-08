package com.concert.model;

import com.concert.grpc.ReservationStatus;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Represents a ticket reservation in the system.
 */
public class Reservation implements Serializable {
    private String id;
    private String concertId;
    private String customerName;
    private String customerEmail;
    private String seatTier;
    private int seatQuantity;
    private boolean includesAfterParty;
    private int afterPartyTickets;
    private double totalPrice;
    private String reservationDate;
    private String paymentMethod;
    private ReservationStatus status;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;

    public Reservation() {
        this.id = UUID.randomUUID().toString();
        this.reservationDate = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        this.status = ReservationStatus.PENDING;
        this.creationTimestamp = System.currentTimeMillis();
        this.lastUpdatedTimestamp = this.creationTimestamp;
    }

    public Reservation(String concertId, String customerName, String customerEmail, String seatTier,
                       int seatQuantity, boolean includesAfterParty, int afterPartyTickets,
                       double totalPrice, String paymentMethod) {
        this();
        this.concertId = concertId;
        this.customerName = customerName;
        this.customerEmail = customerEmail;
        this.seatTier = seatTier;
        this.seatQuantity = seatQuantity;
        this.includesAfterParty = includesAfterParty;
        this.afterPartyTickets = afterPartyTickets;
        this.totalPrice = totalPrice;
        this.paymentMethod = paymentMethod;
    }

    // Convert from gRPC Reservation to domain Reservation
    public static Reservation fromGrpcReservation(com.concert.grpc.Reservation grpcReservation) {
        Reservation reservation = new Reservation();
        reservation.setId(grpcReservation.getId());
        reservation.setConcertId(grpcReservation.getConcertId());
        reservation.setCustomerName(grpcReservation.getCustomerName());
        reservation.setCustomerEmail(grpcReservation.getCustomerEmail());
        reservation.setSeatTier(grpcReservation.getSeatTier());
        reservation.setSeatQuantity(grpcReservation.getSeatQuantity());
        reservation.setIncludesAfterParty(grpcReservation.getIncludesAfterParty());
        reservation.setAfterPartyTickets(grpcReservation.getAfterPartyTickets());
        reservation.setTotalPrice(grpcReservation.getTotalPrice());
        reservation.setReservationDate(grpcReservation.getReservationDate());
        reservation.setPaymentMethod(grpcReservation.getPaymentMethod());
        reservation.setStatus(grpcReservation.getStatus());
        return reservation;
    }

    // Convert to gRPC Reservation
    public com.concert.grpc.Reservation toGrpcReservation() {
        return com.concert.grpc.Reservation.newBuilder()
                .setId(this.id)
                .setConcertId(this.concertId)
                .setCustomerName(this.customerName)
                .setCustomerEmail(this.customerEmail)
                .setSeatTier(this.seatTier)
                .setSeatQuantity(this.seatQuantity)
                .setIncludesAfterParty(this.includesAfterParty)
                .setAfterPartyTickets(this.afterPartyTickets)
                .setTotalPrice(this.totalPrice)
                .setReservationDate(this.reservationDate)
                .setPaymentMethod(this.paymentMethod)
                .setStatus(this.status)
                .build();
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

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getCustomerEmail() {
        return customerEmail;
    }

    public void setCustomerEmail(String customerEmail) {
        this.customerEmail = customerEmail;
    }

    public String getSeatTier() {
        return seatTier;
    }

    public void setSeatTier(String seatTier) {
        this.seatTier = seatTier;
    }

    public int getSeatQuantity() {
        return seatQuantity;
    }

    public void setSeatQuantity(int seatQuantity) {
        this.seatQuantity = seatQuantity;
    }

    public boolean isIncludesAfterParty() {
        return includesAfterParty;
    }

    public void setIncludesAfterParty(boolean includesAfterParty) {
        this.includesAfterParty = includesAfterParty;
    }

    public int getAfterPartyTickets() {
        return afterPartyTickets;
    }

    public void setAfterPartyTickets(int afterPartyTickets) {
        this.afterPartyTickets = afterPartyTickets;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public String getReservationDate() {
        return reservationDate;
    }

    public void setReservationDate(String reservationDate) {
        this.reservationDate = reservationDate;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public void setStatus(ReservationStatus status) {
        this.status = status;
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
        return "Reservation{" +
                "id='" + id + '\'' +
                ", concertId='" + concertId + '\'' +
                ", customerName='" + customerName + '\'' +
                ", customerEmail='" + customerEmail + '\'' +
                ", seatTier='" + seatTier + '\'' +
                ", seatQuantity=" + seatQuantity +
                ", includesAfterParty=" + includesAfterParty +
                ", afterPartyTickets=" + afterPartyTickets +
                ", totalPrice=" + totalPrice +
                ", status=" + status +
                '}';
    }
}