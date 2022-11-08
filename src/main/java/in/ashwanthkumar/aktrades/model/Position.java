package in.ashwanthkumar.aktrades.model;

import com.google.common.base.Preconditions;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

@RequiredArgsConstructor(staticName = "of")
@Getter
@EqualsAndHashCode
@ToString
public class Position {
    private final String ticker;
    private final OrderOp op;
    @With
    private final int quantity;
    @With
    private final double price;

    public PositionExecutionResult exitWith(Order order) {
        Preconditions.checkArgument(StringUtils.equals(this.ticker, order.getTicker()), "Can't add positions of various tickers");
        Preconditions.checkArgument(op != order.getOp(), "Can't merge Positions of same op %s together since the values would be lost", op.name());

        double newOrExistingPrice = (order.getQuantity() > quantity) ? order.getPrice() : price;
        OrderOp newOrExistingOp = (order.getQuantity() > quantity) ? order.getOp() : op;
        Position newPosition = Position.of(ticker, newOrExistingOp, quantity - order.getQuantity(), newOrExistingPrice);

        double pnl = 0.0;
        int qtyForPnl = Math.min(order.getQuantity(), quantity);
        if (op == OrderOp.BUY) {
            pnl = (order.getPrice() - price) * qtyForPnl;
        } else if (op == OrderOp.SELL) {
            pnl = (price - order.getPrice()) * qtyForPnl;
        }

        return new PositionExecutionResult(newPosition, pnl);
    }
}
