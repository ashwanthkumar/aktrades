package in.ashwanthkumar.aktrades;

public enum OrderType {
    // Market Order, we'll fulfil at the order at LTP (Last Traded Price)
    MARKET,

    // Limit Order, depending on if it's a BUY or SELL, we'll wait for
    // this price to be breached on either side to execute the order.
    LIMIT,

    // SL order with a Limit.
    SL_LIMIT,

    // NB: We don't support SL-M because it is not supported on NSE today
}
