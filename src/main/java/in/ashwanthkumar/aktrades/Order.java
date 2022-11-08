package in.ashwanthkumar.aktrades;

/**
 * Represents each item in the Order Book
 */
public class Order {
    // Name of the ticker, we identify the scrip and maintain positions using this
    private final String ticker;
    // quantity in the order
    // TODO(Ashwanth): Need to add validations for lot size and freeze quantity
    private final int quantity;
    // Buy / Sell
    private final OrderOp op;
    // Price at which we want to enter this order.
    // Depending on the strategy of LIMIT or MARKET
    private final double price;

    public Order(String ticker, int quantity, OrderOp op, double price) {
        this.ticker = ticker;
        this.quantity = quantity;
        this.op = op;
        this.price = price;
    }

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
