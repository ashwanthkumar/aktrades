package in.ashwanthkumar.aktrades;


import in.ashwanthkumar.aktrades.model.Order;

import java.time.LocalTime;
import java.util.List;
import java.util.Queue;

public class IntraDayOrderStateMachine {

    private Queue<LocalTime> ticks;
    private Strategy strategy;
    private Queue<Order> openOrders;

    public void execute() {
        while (!ticks.isEmpty()) {
            LocalTime tick = ticks.poll();
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
