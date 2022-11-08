package in.ashwanthkumar.aktrades;

import tech.tablesaw.api.Row;

import java.time.LocalDateTime;
import java.util.Optional;

public class Scrip {
    // Unique identifier of the Scrip aka Symbol in the wild
    private final String name;
    private final Row row;

    public Scrip(String name, Row row) {
        this.name = name;
        this.row = row;
    }

    /**
     * Name of this Scrip
     *
     * @return Name of this Scrip
     */
    public String getName() {
        return name;
    }

    /**
     * Current time of the day
     *
     * @return Current time of the day
     */
    public LocalDateTime instant() {
        return row.getDateTime("dt");
    }

    /**
     * Open Price on this tick
     *
     * @return Open Price on this tick
     */
    public double open() {
        return row.getDouble("Open");
    }

    /**
     * High Price on this tick
     *
     * @return High Price on this tick
     */
    public double high() {
        return row.getDouble("High");
    }

    /**
     * Low price on this tick
     *
     * @return Low price on this tick
     */
    public double low() {
        return row.getDouble("Low");
    }

    /**
     * Close price on this tick
     *
     * @return Close price on this tick
     */
    public double close() {
        return row.getDouble("Close");
    }

    /**
     * Open Interest if present for the scrip
     *
     * @return Open Interest if present for the scrip
     */
    public Optional<Long> oi() {
        try {
            return Optional.of(row.getLong("Open Interest"));
        } catch (IllegalStateException notAvailable) {
            return Optional.empty();
        }
    }

    /**
     * Volume if present for the Scrip
     *
     * @return Volume if present for the Scrip
     */
    public Optional<Long> volume() {
        try {
            return Optional.of(row.getLong("Volume"));
        } catch (IllegalStateException notAvailable) {
            return Optional.empty();
        }
    }
}
