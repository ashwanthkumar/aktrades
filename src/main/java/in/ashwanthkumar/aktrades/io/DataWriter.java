package in.ashwanthkumar.aktrades.io;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.StringUtils;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class DataWriter {
    private final Table table;
    private final File output;

    /**
     * Writes the underlying Table to the output.
     *
     * @param categoricalColumns List of StringColumns in the table that have categorical values.
     *                           These values have less cardinality and are persisted using dictionary
     *                           encoding to save space.
     */
    public void write(Set<String> categoricalColumns) {
        AtomicInteger dictId = new AtomicInteger(1);
        try (BufferAllocator allocator = new RootAllocator()) {
            DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
            List<FieldVector> valueVectors = new ArrayList<>();

            for (Column<?> column : table.columns()) {
                if (ColumnType.valueOf(column.type().name()).equals(ColumnType.DOUBLE)) {
                    Float4Vector arrowColumn = new Float4Vector(column.name(), allocator);
                    arrowColumn.allocateNew(column.size());
                    for (int i = 0; i < column.size(); i++) {
                        Double fromRow = (Double) column.get(i);
                        if (fromRow == null) {
                            arrowColumn.setNull(i);
                        } else {
                            arrowColumn.set(i, fromRow.floatValue());
                        }
                    }
                    arrowColumn.setValueCount(column.size());
                    valueVectors.add(arrowColumn);
                } else if (ColumnType.valueOf(column.type().name()).equals(ColumnType.STRING)) {
                    boolean isCategorical = categoricalColumns.contains(column.name());

                    final VarCharVector dictVector = new VarCharVector(column.name() + "-dict", allocator);
                    dictVector.allocateNew();
                    if (isCategorical) {
                        StringColumn uniqueValues = (StringColumn) column.unique();
                        for (int i = 0; i < uniqueValues.size(); i++) {
                            dictVector.set(i, new Text(uniqueValues.getString(i)));
                        }
                        dictVector.setValueCount(uniqueValues.size());
                    }
                    Dictionary catDictionary = new Dictionary(dictVector, new DictionaryEncoding(dictId.getAndIncrement(), false, new ArrowType.Int(16, false)));
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
                        UInt2Vector encodedVector = (UInt2Vector) DictionaryEncoder.encode(arrowColumn, catDictionary);
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
                    TimeStampSecVector arrowColumn = new TimeStampSecVector(column.name(), allocator);
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
                 FileOutputStream out = new FileOutputStream(output);
                 ArrowFileWriter writer = new ArrowFileWriter(root, dictProvider, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
                log.debug("Bytes written: " + writer.bytesWritten());
                log.debug("Rows written: " + root.getRowCount());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // this is very important, we have to manually deallocate the memory in Root Allocator
            // else it would result in memory leaks. This is because these are memory allocated
            // outside the JVM.
            allocator.releaseBytes(allocator.getAllocatedMemory());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
