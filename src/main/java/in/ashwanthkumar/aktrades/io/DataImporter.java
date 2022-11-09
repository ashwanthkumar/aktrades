package in.ashwanthkumar.aktrades.io;

import in.ashwanthkumar.aktrades.plugins.TableTransformation;
import in.ashwanthkumar.aktrades.plugins.TelegramNfBnfTransformation;
import tech.tablesaw.api.Table;

import java.io.File;
import java.util.Set;

public class DataImporter {
    // We shouldn't be doing any changes to the table ideally after the initial data transformation
    // even if we do, this should be treated as immutable, but some internal implementations aren't
    // immutable like removeColumns, addColumns, etc.
    private final Table table;

    public DataImporter(Table table, TableTransformation transformation) {
        // we store a copy in our reference, so we're not affected by any lingering dataTransformation copies held elsewhere
        this.table = transformation.transform(table).copy();
    }

    public static DataImporter fromCsv(String path, TableTransformation transformer) {
        Table t = Table.read().file(path);
        return new DataImporter(t, transformer);
    }

    public Table getTable() {
        return table;
    }

    public static void main(String[] args) {

        DataImporter dataImporter = DataImporter.fromCsv("/Users/ashwanthkumar/trading/data/28_OCT_03_NOV_WEEKLY_expiry_data_VEGE_NF_AND_BNF_Options_Vege.csv", new TelegramNfBnfTransformation());
        Table table = dataImporter.getTable();
        // we use dictionary to encode these less cardinality columns to save space.
        Set<String> categoricalColumns = Set.of("Ticker", "Day");
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
