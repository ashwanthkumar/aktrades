package in.ashwanthkumar.aktrades.io;

import org.apache.commons.lang3.StringUtils;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class DataImporter {
    // We shouldn't be doing any changes to the table ideally after the initial data transformation
    // even if we do, this should be treated as immutable, but some internal implementations aren't
    // immutable like removeColumns, addColumns, etc.
    private final Table table;

    public DataImporter(Table table, Function<Table, Table> dataTransformation) {
        // we store a copy in our reference so we're not affected by any lingering dataTransformation copies held elsewhere
        this.table = dataTransformation.apply(table).copy();
    }

    public static DataImporter fromCsv(String path, Function<Table, Table> dataTransformation) {
        Table t = Table.read().file(path);
        return new DataImporter(t, dataTransformation);
    }

    public Table getTable() {
        return table;
    }

    public static void main(String[] args) {
        DataImporter dataImporter = DataImporter.fromCsv("/Users/ashwanthkumar/trading/data/28_OCT_03_NOV_WEEKLY_expiry_data_VEGE_NF_AND_BNF_Options_Vege.csv", t -> {
            System.out.println(t.summary());

            // Step 1: Parse the Date/Time as LocalDateTime
            StringColumn dateTimeCol = t.column("Date/Time").asStringColumn();
            DateTimeColumn parsedDateTime = dateTimeCol
                    .mapInto(input -> LocalDateTime.parse(input, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")), DateTimeColumn.create("Time", dateTimeCol.size()));
            t.addColumns(parsedDateTime);
            t.removeColumns("Date/Time");

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
        });
        Table table = dataImporter.getTable();
        System.out.println(table.summary());

        // we use dictionary to encode these less cardinality columns to save space.
        Set<String> categoricalColumns = Set.of("Ticker");

        // Write the data to disk
        new DataWriter(table, new File("output-foo.bin")).write(categoricalColumns);

        // read the data from disk
        Table read = new DataReader(new File("output-foo.bin")).read();
        System.out.println(read.summary());


//        DoubleColumn closeCol = (DoubleColumn) table.column("Close");
//
//        DateTimeColumn dt = (DateTimeColumn) table.column("dt");
//        DateColumn dates = dt.map(LocalDateTime::toLocalDate, DateColumn::create).unique().sorted(Comparator.naturalOrder());
//        // Running for each day
//        for (LocalDate date : dates) {
//            Selection dateSelection = dt.isEqualTo(date.atStartOfDay());
//            TimeColumn timeTicksDuringTheDay = dt.where(dateSelection).map(t -> LocalTime.of(t.getHour(), t.getMinute(), t.getSecond()), TimeColumn::create).unique().sorted(Comparator.naturalOrder());
//
//            for (LocalTime time : timeTicksDuringTheDay) {
//                Selection rowSelection = dt.isEqualTo(date.atTime(time));
//                // TODO: Implement the strategy execution
//            }
//        }
    }
}
