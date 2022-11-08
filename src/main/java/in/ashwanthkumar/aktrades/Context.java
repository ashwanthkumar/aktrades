package in.ashwanthkumar.aktrades;

import in.ashwanthkumar.aktrades.model.Order;
import in.ashwanthkumar.aktrades.model.Position;
import in.ashwanthkumar.aktrades.model.PositionExecutionResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;

// TODO: Engine that the Strategy has access to for querying live strategy info like:
//   - [x] current open positions
//   - [x] past order history (only for intra day)
//   - Any custom configurations or metadata that was passed
//   - View (and edit?) triggers during the live strategy
@Getter
@RequiredArgsConstructor
public class Context {
    private final double perOrderBrokerage;
    private final Map<String, Queue<Position>> positions = new TreeMap<>(Comparator.naturalOrder());
    private final List<Order> orderBook = new LinkedList<>();
    private double pnl = 0.0;
    private double charges = 0.0; // we'll add a fixed charge for each order execution

    public int activePositions() {
        return positions.size();
    }

    /**
     * @param order Order to execute
     */
    void execute(Order order, double ltp) {
        orderBook.add(order);
        Queue<Position> currentPositions = positions.getOrDefault(order.getTicker(), new LinkedList<>());
        charges += this.perOrderBrokerage;
        if (currentPositions.isEmpty()) {
            // TODO: May be support slippage during order execution?
            Position newPosition = Position.of(order.getTicker(), order.getOp(), order.getQuantity(), ltp);
            currentPositions.add(newPosition);
        } else {
            boolean orderExecuted = false;
            while (!orderExecuted && !currentPositions.isEmpty()) {
                Position position = currentPositions.poll();
                PositionExecutionResult result = position.exitWith(order.withPrice(ltp));
                this.pnl += result.getPnl();

                Position newPosition = result.getPosition();
                orderExecuted = result.orderExecuteFully();
                if (!orderExecuted) {
                    // update order ref when it's pending more
                    order = order.withQuantity(Math.abs(newPosition.getQuantity()));
                }

                if (newPosition.getQuantity() != 0) {
                    currentPositions.add(newPosition);
                }
            }
        }

        if (!currentPositions.isEmpty()) {
            positions.put(order.getTicker(), currentPositions);
        } else {
            positions.remove(order.getTicker());
        }
    }
}
