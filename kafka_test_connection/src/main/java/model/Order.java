package model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Order {
    private static final String SEPARATOR = ";";

    @JsonProperty("orderId")
    public String orderId;
    @JsonProperty("amount")
    public int amount;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return amount == order.amount &&
                Objects.equals(orderId, order.orderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, amount);
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
