import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.calcite.com.google.common.collect.Lists;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ParseStringUdtf extends TableFunction<Row> {

    private IStringParser parser;
    private List<String> fieldNames = Lists.newArrayList();
    private List<Class> clazzes = Lists.newArrayList();

    public ParseStringUdtf(String parserClassName) {
        try {
            Class<? extends IStringParser> parserClass = Class.forName(parserClassName).asSubclass(IStringParser.class);
            parser = parserClass.newInstance();
            Field[] fields = parser.getToClass().getDeclaredFields();
            for (Field field : fields) {
                fieldNames.add(field.getName());
                clazzes.add(field.getType());
            }
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
        Collection<Object> objects = parser.parse(message);
        Collection<Row> rows = convertToRows(objects);
        for (Row row : rows) {
            collect(row);
        }
    }

    private Collection<Row> convertToRows(Collection<Object> objects) {
        List<Row> rows = new ArrayList<>();
        for (Object object : objects) {
            int length = fieldNames.size();
            Row row = new Row(length);
            String json = JSON.toJSONString(object);
            JSONObject jsonObject = JSON.parseObject(json);
            for (int i = 0; i < length; i++) {
                String fieldName = fieldNames.get(i);
                row.setField(i, jsonObject.get(fieldName));
            }
            rows.add(row);
        }
        return rows;
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        TypeInformation[] typeInformations = new TypeInformation[clazzes.size()];
        for (int i = 0; i < clazzes.size(); i++) {
            typeInformations[i] = BasicTypeInfo.of(clazzes.get(i));
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
