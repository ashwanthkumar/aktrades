package in.ashwanthkumar.aktrades.io;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@RequiredArgsConstructor
@Slf4j
public class DataReader {
    private final File input;

    public Table read() {
        try (BufferAllocator allocator = new RootAllocator(); FileInputStream fileInputStream = new FileInputStream(input); ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator)) {
            List<Column<?>> columns = new ArrayList<>();

            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
                List<Field> fields = vectorSchemaRoot.getSchema().getFields();
                for (Field field : fields) {
                    FieldVector fieldVector = vectorSchemaRoot.getVector(field);
                    System.out.println(field.getName() + " has " + fieldVector.getValueCount() + " elements of type: " + field.getFieldType().getType().getTypeID());
                    // does the field use a dict based encoding?
                    DictionaryEncoding dictEncoding = field.getDictionary();
                    if (dictEncoding != null) {
                        Dictionary dictionary = reader.getDictionaryVectors().get(dictEncoding.getId());
                        try (ValueVector fieldValues = DictionaryEncoder.decode(fieldVector, dictionary)) {
                            columns.add(
                                    parseColumn(fieldValues, field, StringColumn::create, text -> ((Text) text).toString())
                            );
                        }
                    } else {
                        switch (field.getFieldType().getType().getTypeID()) {
                            case Utf8:
                                columns.add(
                                        parseColumn(fieldVector, field, StringColumn::create, value -> ((Text)value).toString())
                                );
                                break;

                            case FloatingPoint:
                                columns.add(
                                        parseColumn(fieldVector, field, DoubleColumn::create, value -> truncateDecimal(((Float) value).doubleValue(), 2))
                                );
                                break;
                            case Int:
                                columns.add(
                                        parseColumn(fieldVector, field, IntColumn::create, Function.identity())
                                );
                                break;
                            case Timestamp:
                                columns.add(
                                        parseColumn(fieldVector, field, DateTimeColumn::create, Function.identity())
                                );
                                break;
                        }
                    }
                }
            }

            return Table.create(columns);
        } catch (IOException e) {

            throw new RuntimeException(e);
        }
    }

    private Column<?> parseColumn(ValueVector fieldVector, Field field, Function<String, Column<?>> createColumnFn, Function<Object, Object> parseValueFn) {
        Column<?> column = createColumnFn.apply(field.getName());
        for (int i = 0; i < fieldVector.getValueCount(); i++) {
            if (fieldVector.isNull(i)) {
                column.appendMissing();
            } else {
                column.appendObj(parseValueFn.apply(fieldVector.getObject(i)));
            }
        }
        return column;
    }

    private Double truncateDecimal(double x, int numberOfDecimals) {
        if (x > 0) {
            return new BigDecimal(String.valueOf(x)).setScale(numberOfDecimals, RoundingMode.FLOOR).doubleValue();
        } else {
            return new BigDecimal(String.valueOf(x)).setScale(numberOfDecimals, RoundingMode.CEILING).doubleValue();
        }
    }

}
