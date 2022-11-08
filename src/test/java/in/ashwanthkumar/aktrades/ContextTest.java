package in.ashwanthkumar.aktrades;

import in.ashwanthkumar.aktrades.model.Order;
import in.ashwanthkumar.aktrades.model.OrderOp;
import in.ashwanthkumar.aktrades.model.OrderType;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ContextTest {
    @Test
    public void testLongEntryAndExit() {
        Context ctx = new Context(20);
        ctx.execute(Order.of("NIFTY", OrderOp.BUY, OrderType.MARKET, 1, 0.0), 11.0);
        ctx.execute(Order.of("NIFTY", OrderOp.SELL, OrderType.MARKET, 1, 0.0), 15.0);
        assertThat(ctx.getPnl(), is(4.0));
        assertThat(ctx.getCharges(), is(40.0));
        assertThat(ctx.activePositions(), is(0));
        assertThat(ctx.getOrderBook().size(), is(2));
    }

    @Test
    public void testShortEntryAndExit() {
        Context ctx = new Context(20);
        ctx.execute(Order.of("NIFTY", OrderOp.SELL, OrderType.MARKET, 1, 0.0), 11.0);
        ctx.execute(Order.of("NIFTY", OrderOp.BUY, OrderType.MARKET, 1, 0.0), 15.0);
        assertThat(ctx.getPnl(), is(-4.0));
        assertThat(ctx.getCharges(), is(40.0));
        assertThat(ctx.activePositions(), is(0));
        assertThat(ctx.getOrderBook().size(), is(2));
    }

    @Test
    public void testOnlyEntryLong() {
        Context ctx = new Context(20);
        ctx.execute(Order.of("NIFTY", OrderOp.BUY, OrderType.MARKET, 1, 0.0), 11.0);
        assertThat(ctx.getPnl(), is(0.0));
        assertThat(ctx.getCharges(), is(20.0));
        assertThat(ctx.activePositions(), is(1));
        assertThat(ctx.getOrderBook().size(), is(1));
    }

    @Test
    public void testOnlyEntryShort() {
        Context ctx = new Context(20);
        ctx.execute(Order.of("NIFTY", OrderOp.SELL, OrderType.MARKET, 1, 0.0), 11.0);
        assertThat(ctx.getPnl(), is(0.0));
        assertThat(ctx.getCharges(), is(20.0));
        assertThat(ctx.activePositions(), is(1));
        assertThat(ctx.getOrderBook().size(), is(1));
    }

}