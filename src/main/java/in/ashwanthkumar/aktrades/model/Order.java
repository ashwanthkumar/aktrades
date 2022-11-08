package in.ashwanthkumar.aktrades.model;

import lombok.RequiredArgsConstructor;
import lombok.With;

/**
 * Represents each item in the Order Book
 */
@RequiredArgsConstructor(staticName = "of")
public class Order {
    // Name of the ticker, we identify the scrip and maintain positions using this
    private final String ticker;
    // Buy / Sell
    private final OrderOp op;
    /**
     * Type of this order
     */
    private final OrderType orderType;
    // quantity in the order
    // TODO(Ashwanth): Need to add validations for lot size and freeze quantity
    @With
    private final int quantity;
    // Price at which we want to enter this order.
    // Depending on the strategy of LIMIT or MARKET
    // If MARKET, we ignore this price value.
    @With
    private final double price;

    public String getTicker() {
        return ticker;
    }

    public int getQuantity() {
        return quantity;
    }

    public OrderOp getOp() {
        return op;
    }

    public double getPrice() {
        return price;
    }
}
