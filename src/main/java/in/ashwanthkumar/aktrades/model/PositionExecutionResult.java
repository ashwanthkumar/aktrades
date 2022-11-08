package in.ashwanthkumar.aktrades.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

@RequiredArgsConstructor
@Getter
public class PositionExecutionResult {
    // new position if any after the order execution
    @NonNull
    private final Position position;
    // Pnl from executing an order against a position
    private final double pnl;

    public boolean orderExecuteFully() {
        return position.getQuantity() >= 0;
    }
}
