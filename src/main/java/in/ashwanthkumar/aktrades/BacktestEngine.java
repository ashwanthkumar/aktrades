package in.ashwanthkumar.aktrades;


import in.ashwanthkumar.aktrades.model.Order;
import lombok.RequiredArgsConstructor;

import java.time.LocalTime;
import java.util.List;
import java.util.Queue;

@RequiredArgsConstructor
public class BacktestEngine {
    private final List<LocalTime> ticks;
    private final Strategy strategy;
    private final Context context;
    private final Queue<Order> openOrders;

    public void execute() {
        for (LocalTime tick : ticks) {
            // Check for open orders that we need to execute
            executeOpenOrders();

            // TODO: Read or fetch the required data - Use DataLoader
            // execute the strategy for the given tick
            List<Order> newOrders = strategy.onTick(null, null);
            openOrders.addAll(newOrders);
        }

        // Cancel the open orders
    }

    private void executeOpenOrders() {
        // TODO: We would be executing all the MARKET orders first
        //   followed by LIMIT orders that can be executed. SL orders
        //   that have satisfied the trigger condition, will be converted
        //   to MARKET orders and executed on the next tick.
        throw new RuntimeException("TODO: Implement execute Open Orders");
    }
}
