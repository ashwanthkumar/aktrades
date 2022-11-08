package in.ashwanthkumar.aktrades.model;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class PositionTest {
    @Test(expected = IllegalArgumentException.class)
    public void testMergeOnDifferentTickersShouldThrow() {
        Position.of("NIFTY", OrderOp.BUY, 1, 10.0)
                .exitWith(Order.of("BANKNIFTY", OrderOp.SELL, OrderType.LIMIT, 1, 15.0));
    }

    @Test
    public void testMergeWithSameQuantities() {
        Position p1 = Position.of("NIFTY", OrderOp.BUY, 1, 10.0);

        PositionExecutionResult result = p1.exitWith(Order.of("NIFTY", OrderOp.SELL, OrderType.LIMIT, 1, 15.0));
        assertThat(result.getPnl(), is(5.0));
        assertThat(result.getPosition(), is(p1.withQuantity(0)));
    }

    @Test
    public void testMergeForScaleDown() {
        Position p1 = Position.of("NIFTY", OrderOp.BUY, 5, 10.0);

        PositionExecutionResult result = p1.exitWith(Order.of("NIFTY", OrderOp.SELL, OrderType.LIMIT, 2, 15.0));
        assertThat(result.getPnl(), is(10.0));
        assertThat(result.getPosition(), is(p1.withQuantity(3)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeForScaleUp() {
        Position p1 = Position.of("NIFTY", OrderOp.BUY, 5, 10.0);
        p1.exitWith(Order.of("NIFTY", OrderOp.BUY, OrderType.LIMIT, 2, 15.0));
    }

    /**
     * This scenario is done when we've scaled up over time but want to exit in bulk
     */
    @Test
    public void testMergeForMoreScaleDownThanPresent() {
        String ticker = "NIFTY";
        Position p1 = Position.of(ticker, OrderOp.BUY, 5, 10.0);

        PositionExecutionResult result = p1.exitWith(Order.of(ticker, OrderOp.SELL, OrderType.LIMIT, 10, 15.0));
        assertThat(result.getPnl(), is(25.0));
        assertThat(result.getPosition(), is(Position.of(ticker, OrderOp.SELL, -5, 15.0)));
    }
}