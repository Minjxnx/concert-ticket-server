package com.concert.model;

import java.io.Serializable;

/**
 * Represents a seat tier in a concert (e.g., Regular, VIP).
 */
public class Seat implements Serializable {
    private String name;
    private double price;
    private int totalSeats;
    private int availableSeats;

    public Seat() {
    }

    public Seat(String name, double price, int totalSeats, int availableSeats) {
        this.name = name;
        this.price = price;
        this.totalSeats = totalSeats;
        this.availableSeats = availableSeats;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getTotalSeats() {
        return totalSeats;
    }

    public void setTotalSeats(int totalSeats) {
        this.totalSeats = totalSeats;
    }

    public int getAvailableSeats() {
        return availableSeats;
    }

    public void setAvailableSeats(int availableSeats) {
        this.availableSeats = availableSeats;
    }

    @Override
    public String toString() {
        return "Seat{" +
                "name='" + name + '\'' +
                ", price=" + price +
                ", totalSeats=" + totalSeats +
                ", availableSeats=" + availableSeats +
                '}';
    }
}