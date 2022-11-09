package in.ashwanthkumar.aktrades.plugins;

import org.apache.commons.lang3.StringUtils;
import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class TelegramNfBnfTransformation implements TableTransformation {

    private static final String NIFTY_WK_PREFIX = "NIFTYWK";
    private static final String BANKNIFTY_WK_PREFIX = "BANKNIFTYWK";

    @Override
    public Table transform(Table table) {
        System.out.println(table.summary());

        // Step 1: Parse the Date/Time as LocalDateTime
        StringColumn dateTimeCol = table.column("Date/Time").asStringColumn();
        DateTimeColumn parsedDateTime = dateTimeCol
                .mapInto(
                        input -> LocalDateTime.parse(input, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
                        DateTimeColumn.create("Time", dateTimeCol.size())
                );
        table.addColumns(parsedDateTime);
        table.removeColumns("Date/Time");

        StringColumn dayCol = parsedDateTime.mapInto(localDateTime -> localDateTime.getDayOfWeek().name(), StringColumn.create("Day", parsedDateTime.size()));
        table.addColumns(dayCol);

        // Step 2: Keep only the required rows:
        Predicate<LocalDateTime> withinFnOTimeRange = localDateTime -> localDateTime.getHour() < 15 || (localDateTime.getHour() == 15 && localDateTime.getMinute() <= 29);
        Selection activeTradingHours = parsedDateTime.eval(withinFnOTimeRange);

        StringColumn tickerCol = table.column("Ticker").asStringColumn();
        boolean isWeekly = tickerCol.anyMatch(ticker -> ticker.contains(NIFTY_WK_PREFIX) || ticker.contains(BANKNIFTY_WK_PREFIX));
        BooleanColumn isWeeklyCol = tickerCol.mapInto(s -> isWeekly, BooleanColumn.create("IsWeekly", tickerCol.size()));
        table.addColumns(isWeeklyCol);

        Predicate<String> isNfOrBnf = ticker -> {
            Set<String> bnfIndex = Set.of("BANKNIFTY", "BANKNIFTY-FUT");
            Set<String> nfIndex = Set.of("NIFTY", "NIFTY-FUT");

            return nfIndex.contains(ticker) || StringUtils.startsWith(ticker, "NIFTYWK") ||
                    bnfIndex.contains(ticker) || StringUtils.startsWith(ticker, "BANKNIFTYWK");
        };
        Selection onlyNfAndBnf = tickerCol.eval(isNfOrBnf);

        // return a subset of table that matches all the required selections
        return table.where(onlyNfAndBnf.and(activeTradingHours));
    }
}
