package in.ashwanthkumar.aktrades.io;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.StringUtils;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.selection.Selection;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataImporter {
    public static final String NA = "N/A";

    // We shouldn't be doing any changes to the table ideally after the initial data transformation
    // even if we do, this should be treated as immutable, but some internal implementations aren't
    // immutable like removeColumns, addColumns, etc.
    private final Table table;

    public DataImporter(Table table, Function<Table, Table> dataTransformation) {
        this.table = dataTransformation.apply(table);
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

            // Step 1: Parse the Date/Time as LocalDateTime and remove rows that don't make sense
            StringColumn dateTimeCol = t.column("Date/Time").asStringColumn();
            DateTimeColumn parsedDateTime = dateTimeCol
                    .mapInto(input -> LocalDateTime.parse(input, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")), DateTimeColumn.create("Time", dateTimeCol.size()));
            t.addColumns(parsedDateTime);
            t.removeColumns("Date/Time");
            Predicate<LocalDateTime> withinFnOTimeRange = localDateTime -> localDateTime.getHour() < 15 || (localDateTime.getHour() == 15 && localDateTime.getMinute() <= 29);
            Selection filterRowsBeyondFnOClose = parsedDateTime.eval(withinFnOTimeRange.negate());
            t = t.dropWhere(filterRowsBeyondFnOClose);

            // Step 2: Compute the underlying and remove the rows where the underlying is null
            StringColumn tickerCol = t.column("Ticker").asStringColumn();
            StringColumn underlyingCol = StringColumn.create("Underlying", tickerCol.size());
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
        Table table = dataImporter.getTable();
        System.out.println(table.summary());

        // we use dictionary to encode them to save space
        // Also these should only be STRING column types
        Set<String> categoricalColumns = Set.of("Ticker", "Underlying", "OptionType");

        AtomicInteger dictId = new AtomicInteger(1);
        try (BufferAllocator allocator = new RootAllocator()) {
            DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
            List<FieldVector> valueVectors = new ArrayList<>();

            for (Column<?> column : table.columns()) {
                if (ColumnType.valueOf(column.type().name()).equals(ColumnType.DOUBLE)) {
                    Float4Vector arrowColumn = new Float4Vector(column.name(), allocator);
                    arrowColumn.allocateNew(column.size());
                    NullableFloat4Holder nullableValue = new NullableFloat4Holder();
                    for (int i = 0; i < column.size(); i++) {
                        Double fromRow = (Double) column.get(i);
                        if (fromRow == null) {
                            nullableValue.isSet = 0;
                        } else {
                            nullableValue.isSet = 1;
                            nullableValue.value = fromRow.floatValue();
                        }
                        arrowColumn.set(i, nullableValue);
                    }
                    arrowColumn.setValueCount(column.size());
                    arrowColumn.reAlloc();
                    valueVectors.add(arrowColumn);
                } else if (ColumnType.valueOf(column.type().name()).equals(ColumnType.STRING)) {
                    boolean isCategorical = categoricalColumns.contains(column.name());

                    final VarCharVector dictVector = new VarCharVector(column.name() + "-dict", allocator);
                    dictVector.allocateNew();
                    if (isCategorical) {
                        StringColumn uniqueValues = (StringColumn) column.unique();
                        uniqueValues.sortAscending();
                        for (int i = 0; i < uniqueValues.size(); i++) {
                            dictVector.set(i, new Text(uniqueValues.getString(i)));
                        }
                        dictVector.setValueCount(uniqueValues.size());
                    }
                    Dictionary catDictionary = new Dictionary(dictVector, new DictionaryEncoding(dictId.getAndIncrement(), true, /*indexType=*/null));
                    dictProvider.put(catDictionary);


                    VarCharVector arrowColumn = new VarCharVector(column.name(), allocator);
                    double numberOfBytes = column
                            .map(s -> StringUtils.length((String) s), IntColumn::create)
                            .sum();

                    arrowColumn.allocateNew((long) numberOfBytes, column.size());
                    for (int i = 0; i < column.size(); i++) {
                        String fromRow = column.getString(i);
                        if (fromRow == null) {
                            arrowColumn.setNull(i);
                        } else {
                            arrowColumn.set(i, new Text(fromRow));
                        }
                    }
                    arrowColumn.setValueCount(column.size());

                    if (isCategorical) {
                        IntVector encodedVector = (IntVector) DictionaryEncoder.encode(arrowColumn, catDictionary);
                        valueVectors.add(encodedVector);
                        arrowColumn.clear();
                    } else {
                        valueVectors.add(arrowColumn);
                        dictVector.clear();
                    }
                } else if (ColumnType.valueOf(column.type().name()).equals(ColumnType.INTEGER)) {
                    IntVector arrowColumn = new IntVector(column.name(), allocator);
                    arrowColumn.allocateNew(column.size());
                    for (int i = 0; i < column.size(); i++) {
                        Integer fromRow = (Integer) column.get(i);
                        if (fromRow == null) {
                            arrowColumn.setNull(i);
                        } else {
                            arrowColumn.set(i, fromRow);
                        }
                    }
                    arrowColumn.setValueCount(column.size());
                    valueVectors.add(arrowColumn);
                } else if (ColumnType.valueOf(column.type().name()).equals(ColumnType.LOCAL_DATE_TIME)) {
                    BigIntVector arrowColumn = new BigIntVector(column.name(), allocator);
                    arrowColumn.allocateNew(column.size());
                    for (int i = 0; i < column.size(); i++) {
                        LocalDateTime row = (LocalDateTime) column.get(i);
                        if (row == null) {
                            arrowColumn.setNull(i);
                        } else {
                            arrowColumn.set(i, row.toEpochSecond(ZoneOffset.UTC));
                        }
                    }
                    arrowColumn.setValueCount(column.size());
                    valueVectors.add(arrowColumn);
                }
            }
            List<Field> fields = valueVectors.stream().map(ValueVector::getField).collect(Collectors.toList());
            try (VectorSchemaRoot root = new VectorSchemaRoot(fields, valueVectors, table.rowCount());
                 FileOutputStream out = new FileOutputStream("output-foo.bin");
                 ArrowFileWriter writer = new ArrowFileWriter(root, dictProvider, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
                System.out.println("Bytes written: " + writer.bytesWritten());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            allocator.releaseBytes(allocator.getAllocatedMemory());
            allocator.releaseBytes(allocator.getInitReservation());
            // Clear the field vectors so the RootAllocator doesn't throw when closing
            for (FieldVector column : valueVectors) {
                column.clear();
            }
            valueVectors.clear();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


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