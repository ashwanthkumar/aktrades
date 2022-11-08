package in.ashwanthkumar.aktrades;

import java.util.List;

public interface Strategy {
    /**
     * Invoked for each tick (1-min or 5-min). Called once for each scrip during back testing.
     *
     * @param scrip   Scrip data
     * @param context Context object to query runtime info
     * @return Orders that are enqueued to the order book. The order is executed only on the next tick.
     */
    List<Order> onTick(Scrip scrip, StrategyContext context);

    /**
     * Invoked after each order execution, this would be called for each order that is ful-filled
     * by the system or by the broker when we're trading live.
     * <p>
     * TODO: At a later point, may be have filters on what type of orders we want to watch for?
     *   - May be just subscribe to SL orders instead of LIMIT and Market orders?
     * </p>
     *
     * @param order   Order that just got executed
     * @param context Context object to query runtime info
     * @return Orders that are enqueued to the order book. The order is executed only on the next tick.
     */
    List<Order> afterOrder(Order order, StrategyContext context);
}
