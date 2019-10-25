import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class PersonParser implements IStringParser {

    @Override
    public LinkedHashMap<String, ColumnType> getColumnTypesMap() {
        LinkedHashMap<String, ColumnType> map = new LinkedHashMap<>();
        map.put("name", ColumnType.STRING);
        map.put("age", ColumnType.INT);
        map.put("grade", ColumnType.BOOLEAN);
        map.put("scores", ColumnType.INT_ARRAY);
        map.put("colors", ColumnType.STRING_ARRAY);
        return map;
    }

    @Override
    public List<LinkedHashMap<String, Object>> parse(byte[] bytes) {
        String json = new String(bytes, Charset.forName("UTF-8"));
        Person person = JSON.parseObject(json, Person.class);
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("name", person.getName());
        map.put("age", person.getAge());
        map.put("adult", person.isAdult());
        map.put("scores", person.getScores());
        map.put("colors", person.getColors());
        return Arrays.asList(map);
    }
}
