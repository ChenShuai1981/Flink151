import java.io.Serializable;
import java.util.List;

public interface IStringParser extends Serializable {
    List<ColumnType> getColumnTypes();
    List<TableObject> parse(byte[] bytes);
}
