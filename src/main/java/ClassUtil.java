public class ClassUtil {
    public static Class stringConvertClass(String s) {
        switch (s.toUpperCase()) {
            case "INTEGER":
                return Integer.class;
            case "DOUBLE":
                return Double.class;
            case "LONG":
                return Long.class;
            case "FLOAT":
                return Float.class;
            case "BOOLEAN":
                return Boolean.class;
            case "VARCHAR":
            case "STRING":
                default:
                return String.class;
        }
    }
}
