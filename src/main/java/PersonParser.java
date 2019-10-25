import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class PersonParser implements IStringParser {

    @Override
    public List<ColumnType> getColumnTypes() {
        return Arrays.asList(ColumnType.STRING, ColumnType.INT, ColumnType.BOOLEAN, ColumnType.INT_ARRAY, ColumnType.STRING_ARRAY);
    }

    @Override
    public List<TableObject> parse(byte[] bytes) {
        String json = new String(bytes, Charset.forName("UTF-8"));
        Person person = JSON.parseObject(json, Person.class);
        TableObject tableObject = new TableObject();
        ColumnObject column = new ColumnObject("name", ColumnType.STRING, person.getName());
        tableObject.addColumn(column);
        column = new ColumnObject("age", ColumnType.INT, person.getAge());
        tableObject.addColumn(column);
        column = new ColumnObject("grade", ColumnType.BOOLEAN, person.isAdult());
        tableObject.addColumn(column);
        column = new ColumnObject("scores", ColumnType.INT_ARRAY, person.getScores());
        tableObject.addColumn(column);
        column = new ColumnObject("colors", ColumnType.STRING_ARRAY, person.getColors());
        tableObject.addColumn(column);
        return Arrays.asList(tableObject);
    }
}
