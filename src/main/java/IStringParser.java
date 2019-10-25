import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

public interface IStringParser extends Serializable {
    LinkedHashMap<String, ColumnType> getColumnTypesMap();
    List<LinkedHashMap<String, Object>> parse(byte[] bytes);
}
