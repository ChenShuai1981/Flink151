import java.io.Serializable;
import java.util.Collection;

public interface IStringParser<T> extends Serializable {
    Class<T> getToClass();
    Collection<T> parse(byte[] bytes);
}
