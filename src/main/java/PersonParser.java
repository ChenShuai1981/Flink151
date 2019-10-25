import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersonParser implements IStringParser {

    @Override
    public List<ColumnObject> getColumnObjects() {
        List<ColumnObject> columnObjects = new ArrayList<>();
        ColumnObject column = new ColumnObject("name", ColumnType.STRING);
        columnObjects.add(column);
        column = new ColumnObject("age", ColumnType.INT);
        columnObjects.add(column);
        column = new ColumnObject("grade", ColumnType.BOOLEAN);
        columnObjects.add(column);
        column = new ColumnObject("scores", ColumnType.INT_ARRAY);
        columnObjects.add(column);
        column = new ColumnObject("colors", ColumnType.STRING_ARRAY);
        columnObjects.add(column);

        return columnObjects;
    }

    @Override
    public List<TableObject> parse(byte[] bytes) {
        String json = new String(bytes, Charset.forName("UTF-8"));
        Person person = JSON.parseObject(json, Person.class);
        TableObject tableObject = new TableObject();
        List<ColumnObject> columnObjects = getColumnObjects();
        for (ColumnObject columnObject : columnObjects) {
            switch (columnObject.getName()) {
                case "name":
                    columnObject.setValue(person.getName());
                    break;
                case "age":
                    columnObject.setValue(person.getAge());
                    break;
                case "adult":
                    columnObject.setValue(person.isAdult());
                    break;
                case "scores":
                    columnObject.setValue(person.getScores());
                    break;
                case "colors":
                    columnObject.setValue(person.getColors());
                    break;
            }
            tableObject.addColumn(columnObject);
        }
        return Arrays.asList(tableObject);
    }
}
