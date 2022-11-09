package in.ashwanthkumar.aktrades.plugins;

import org.apache.commons.lang3.StringUtils;
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
    @Override
    public Table transform(Table t) {
        System.out.println(t.summary());

        // Step 1: Parse the Date/Time as LocalDateTime
        StringColumn dateTimeCol = t.column("Date/Time").asStringColumn();
        DateTimeColumn parsedDateTime = dateTimeCol
                .mapInto(
                        input -> LocalDateTime.parse(input, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
                        DateTimeColumn.create("Time", dateTimeCol.size())
                );
        t.addColumns(parsedDateTime);
        t.removeColumns("Date/Time");

        StringColumn dayCol = parsedDateTime.mapInto(localDateTime -> localDateTime.getDayOfWeek().name(), StringColumn.create("Day", parsedDateTime.size()));
        t.addColumns(dayCol);

        // Step 2: Keep only the required rows:
        Predicate<LocalDateTime> withinFnOTimeRange = localDateTime -> localDateTime.getHour() < 15 || (localDateTime.getHour() == 15 && localDateTime.getMinute() <= 29);
        Selection activeTradingHours = parsedDateTime.eval(withinFnOTimeRange);

        StringColumn tickerCol = t.column("Ticker").asStringColumn();
        Predicate<String> isNfOrBnf = ticker -> {
            Set<String> bnfIndex = Set.of("BANKNIFTY", "BANKNIFTY-FUT");
            Set<String> nfIndex = Set.of("NIFTY", "NIFTY-FUT");

            return nfIndex.contains(ticker) || StringUtils.startsWith(ticker, "NIFTYWK") ||
                    bnfIndex.contains(ticker) || StringUtils.startsWith(ticker, "BANKNIFTYWK");
        };
        Selection onlyNfAndBnf = tickerCol.eval(isNfOrBnf);

        // return a subset of table that matches all the required selections
        return t.where(onlyNfAndBnf.and(activeTradingHours));
    }
}
