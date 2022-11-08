package in.ashwanthkumar.aktrades;

import org.apache.commons.lang3.StringUtils;
import tech.tablesaw.api.*;
import tech.tablesaw.selection.Selection;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class DataLoader {
    public static final String NA = "N/A";

    // We shouldn't be doing any changes to the table ideally after the initial data transformation
    // even if we do, this should be treated as immutable, but some internal implementations aren't
    // immutable like removeColumns, addColumns, etc.
    private final Table table;

    public DataLoader(Table table, Function<Table, Table> dataTransformation) {
        this.table = dataTransformation.apply(table);
    }

    public static DataLoader fromCsv(String path, Function<Table, Table> dataTransformation) {
        Table t = Table.read().file(path);
        return new DataLoader(t, dataTransformation);
    }

    public Table getTable() {
        return table;
    }

    public static void main(String[] args) {
        DataLoader dataLoader = DataLoader.fromCsv("/Users/ashwanthkumar/trading/data/28_OCT_03_NOV_WEEKLY_expiry_data_VEGE_NF_AND_BNF_Options_Vege.csv", t -> {
            System.out.println(t.summary());

            // Step 1: Parse the Date/Time as LocalDateTime and remove rows that don't make sense
            StringColumn dateTimeCol = t.column("Date/Time").asStringColumn();
            DateTimeColumn parsedDateTime = dateTimeCol
                    .mapInto(input -> LocalDateTime.parse(input, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")), DateTimeColumn.create("dt", dateTimeCol.size()));
            t.addColumns(parsedDateTime);
            t.removeColumns("Date/Time");
            Predicate<LocalDateTime> withinFnOTimeRange = localDateTime -> localDateTime.getHour() < 15 || (localDateTime.getHour() == 15 && localDateTime.getMinute() <= 29);
            Selection filterRowsBeyondFnOClose = parsedDateTime.eval(withinFnOTimeRange.negate());
            t = t.dropWhere(filterRowsBeyondFnOClose);

            // Step 2: Compute the underlying and remove the rows where the underlying is null
            StringColumn tickerCol = t.column("Ticker").asStringColumn();
            StringColumn underlyingCol = StringColumn.create("underlying", tickerCol.size());
            tickerCol.mapInto(ticker -> {
                Set<String> bnfIndex = Set.of("BANKNIFTY", "BANKNIFTY-FUT");
                Set<String> nfIndex = Set.of("NIFTY", "NIFTY-FUT");
                if (nfIndex.contains(ticker) || StringUtils.startsWith(ticker, "NIFTYWK")) {
                    return "NF";
                } else if (bnfIndex.contains(ticker) || StringUtils.startsWith(ticker, "BANKNIFTYWK")) {
                    return "BNF";
                }
                return NA;
            }, underlyingCol);
            Predicate<String> isNA = input -> StringUtils.equals(input, NA);
            Selection removeNAs = underlyingCol.eval(isNA);
            t.addColumns(underlyingCol);
            t = t.dropWhere(removeNAs);

            // Step 3: Parse Option Strikes and Types
            tickerCol = t.column("Ticker").asStringColumn();
            Pattern optionTickerPattern = Pattern.compile("NIFTYWK([0-9]+)(CE|PE)");

            StringColumn optionType = StringColumn.create("OptionType", tickerCol.size());
            tickerCol.mapInto(ticker -> optionTickerPattern.matcher(ticker)
                    .results()
                    .map(mr -> mr.group(2))
                    .findFirst()
                    .orElse(null), optionType);
            t.addColumns(optionType);

            IntColumn optionStrike = IntColumn.create("OptionStrike", tickerCol.size());
            tickerCol.mapInto(ticker -> optionTickerPattern.matcher(ticker)
                    .results()
                    .map(mr -> mr.group(1))
                    .findFirst()
                    .map(Integer::parseInt)
                    .orElse(null), optionStrike);
            t.addColumns(optionStrike);

            return t;
        });
        Table table = dataLoader.getTable();
        System.out.println(table);

        DoubleColumn closeCol = (DoubleColumn) table.column("Close");

        DateTimeColumn dt = (DateTimeColumn) table.column("dt");
        DateColumn dates = dt.map(LocalDateTime::toLocalDate, DateColumn::create).unique().sorted(Comparator.naturalOrder());
        // Running for each day
        for (LocalDate date : dates) {
            Selection dateSelection = dt.isEqualTo(date.atStartOfDay());
            TimeColumn timeTicksDuringTheDay = dt.where(dateSelection).map(t -> LocalTime.of(t.getHour(), t.getMinute(), t.getSecond()), TimeColumn::create).unique().sorted(Comparator.naturalOrder());

            for (LocalTime time : timeTicksDuringTheDay) {
                Selection rowSelection = dt.isEqualTo(date.atTime(time));
                // TODO: Implement the strategy execution
            }
        }
    }
}
