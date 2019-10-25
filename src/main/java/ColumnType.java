import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public enum ColumnType {
    STRING(String.class),
    BYTE(Byte.class),
    BOOLEAN(Boolean.class),
    SHORT(Short.class),
    INT(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    CHAR(Character.class),
    BIG_DEC(BigDecimal.class),
    BIG_INT(BigInteger.class),
    SQL_DATE(Date.class),
    SQL_TIME(Time.class),
    SQL_TIMESTAMP(Timestamp.class),

    STRING_ARRAY(String[].class),

    BYTE_ARRAY(Byte[].class),
    BOOLEAN_ARRAY(Boolean[].class),
    SHORT_ARRAY(Short[].class),
    INT_ARRAY(Integer[].class),
    LONG_ARRAY(Long[].class),
    FLOAT_ARRAY(Float[].class),
    DOUBLE_ARRAY(Double[].class),
    CHAR_ARRAY(Character[].class);

    private Class javaClass;

    ColumnType(Class javaClass) {
        this.javaClass = javaClass;
    }

    public Class getJavaClass() {
        return javaClass;
    }
}
