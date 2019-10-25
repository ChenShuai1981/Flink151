import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.types.Row;

import java.util.*;

import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.*;

public class ParseStringUdtf extends TableFunction<Row> {

    private IStringParser parser;

    private List<ColumnType> columnTypes = null;

    private Map<ColumnType, TypeInformation> typeMapping = ImmutableMap.<ColumnType, TypeInformation>builder()
            .put(ColumnType.STRING, Types.STRING)
            .put(ColumnType.SHORT, Types.SHORT)
            .put(ColumnType.INT, Types.INT)
            .put(ColumnType.LONG, Types.LONG)
            .put(ColumnType.DOUBLE, Types.DOUBLE)
            .put(ColumnType.FLOAT, Types.FLOAT)
            .put(ColumnType.BYTE, Types.BYTE)
            .put(ColumnType.CHAR, Types.CHAR)
            .put(ColumnType.BIG_DEC, Types.BIG_DEC)
            .put(ColumnType.BIG_INT, Types.BIG_INT)
            .put(ColumnType.BOOLEAN, Types.BOOLEAN)
            .put(ColumnType.SQL_DATE, Types.SQL_DATE)
            .put(ColumnType.SQL_TIME, Types.SQL_TIME)
            .put(ColumnType.SQL_TIMESTAMP, Types.SQL_TIMESTAMP)
            .put(ColumnType.STRING_ARRAY, STRING_ARRAY_TYPE_INFO)
            .put(ColumnType.SHORT_ARRAY, SHORT_PRIMITIVE_ARRAY_TYPE_INFO)
            .put(ColumnType.BOOLEAN_ARRAY, BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)
            .put(ColumnType.BYTE_ARRAY, BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
            .put(ColumnType.INT_ARRAY, INT_PRIMITIVE_ARRAY_TYPE_INFO)
            .put(ColumnType.LONG_ARRAY, LONG_PRIMITIVE_ARRAY_TYPE_INFO)
            .put(ColumnType.FLOAT_ARRAY, FLOAT_PRIMITIVE_ARRAY_TYPE_INFO)
            .put(ColumnType.DOUBLE_ARRAY, DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO)
            .put(ColumnType.CHAR_ARRAY, CHAR_PRIMITIVE_ARRAY_TYPE_INFO)
            .build();

    public ParseStringUdtf(String parserClassName) {
        try {
            Class<? extends IStringParser> parserClass = Class.forName(parserClassName).asSubclass(IStringParser.class);
            parser = parserClass.newInstance();
            columnTypes = Collections.unmodifiableList(parser.getColumnTypes());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public void eval(byte[] message) {
        Collection<TableObject> objects = parser.parse(message);
        Collection<Row> rows = convertToRows(objects);
        for (Row row : rows) {
            collect(row);
        }
    }

    private Collection<Row> convertToRows(Collection<TableObject> objects) {
        List<Row> rows = new ArrayList<>();
        for (TableObject object : objects) {
            List<ColumnObject> columns = object.getColumns();
            Row row = new Row(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                ColumnObject column = columns.get(i);
                row.setField(i, column.getValue());
            }
            rows.add(row);
        }
        return rows;
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        TypeInformation[] typeInformations = new TypeInformation[columnTypes.size()];
        for (int i = 0; i < columnTypes.size(); i++) {
            ColumnType columnType = columnTypes.get(i);
            TypeInformation typeInformation = typeMapping.get(columnType);
            typeInformations[i] = typeInformation;
        }
        RowTypeInfo rowType = new RowTypeInfo(typeInformations);
        return new TypeInfoWrappedDataType(rowType);
    }

    // 可选，close方法可不编写。
    @Override
    public void close() throws Exception {
        super.close();
    }
}
