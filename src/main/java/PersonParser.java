import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class PersonParser implements IStringParser<Person> {
    @Override
    public Class<Person> getToClass() {
        return Person.class;
    }

    @Override
    public List<Person> parse(byte[] bytes) {
        String json = new String(bytes, Charset.forName("UTF-8"));
        return Arrays.asList(JSON.parseObject(json, Person.class));
    }
}
