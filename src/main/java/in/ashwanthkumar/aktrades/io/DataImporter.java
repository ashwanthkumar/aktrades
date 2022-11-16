package in.ashwanthkumar.aktrades.io;

import com.google.common.base.Preconditions;
import in.ashwanthkumar.aktrades.plugins.TableTransformation;
import in.ashwanthkumar.aktrades.plugins.TelegramNfBnfTransformation;
import lombok.extern.slf4j.Slf4j;
import tech.tablesaw.api.Table;

import java.io.File;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Slf4j
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

    // Bulk import all the CSV files into our storage.
    public static void main(String[] args) {
        String baseDir = "/Users/ashwanthkumar/trading/data/";
        File[] csvFilesToImport = new File(baseDir).listFiles((dir, name) -> name.endsWith(".csv"));
        Objects.requireNonNull(csvFilesToImport, "We should have CSV Files to import");
        Preconditions.checkState(csvFilesToImport.length > 1, "CSV File seems to be empty");
        File outputFile = new File(String.format("output/%s.bin", UUID.randomUUID().toString()));
        Table tableSoFar = null;
        for (File file : csvFilesToImport) {
            log.info("Loading the file {}", file.getAbsolutePath());
            DataImporter dataImporter = DataImporter.fromCsv(file.getAbsolutePath(), new TelegramNfBnfTransformation());
            Table table = dataImporter.getTable();
            if (tableSoFar == null) {
                tableSoFar = table;
            } else {
                // Note: Use append instead of concat, because concat appends only the columns and not rows.
                tableSoFar = tableSoFar.append(table);
            }
        }

        // we use dictionary to encode these less cardinality columns to save space.
        Set<String> categoricalColumns = Set.of("Ticker", "Day");
        // create the output directory if not exists
        outputFile.getParentFile().mkdirs();

        new DataWriter(tableSoFar, outputFile).write(categoricalColumns);
    }

    // read the data from disk
    //        Table read = new DataReader(new File("output-foo.bin")).read();
    //        System.out.println(read.summary());
}