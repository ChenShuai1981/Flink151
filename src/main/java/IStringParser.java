import java.io.Serializable;
import java.util.List;

public interface IStringParser extends Serializable {
    List<ColumnObject> getColumnObjects();
    List<TableObject> parse(byte[] bytes);
}
