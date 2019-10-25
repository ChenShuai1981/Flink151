//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.shaded.calcite.com.google.common.collect.Lists;
//import org.apache.flink.table.api.functions.TableFunction;
//import org.apache.flink.table.api.types.DataType;
//import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
//import org.apache.flink.types.Row;
//
//import java.nio.charset.Charset;
//import java.util.List;
//
//public class KafkaUDTF extends TableFunction<Row> {
//
//    private List<Class> clazzes = Lists.newArrayList();
//    private List<String> fieldName = Lists.newArrayList();
//
//    public KafkaUDTF(String... args) {
//        for (String arg : args) {
//            if (arg.contains(",")) {
//                //将 "VARCHAR" 转换为 String.class, "INTEGER" 转为 Integer.class等
//                clazzes.add(ClassUtil.stringConvertClass(arg.split(",")[1]));
//                fieldName.add(arg.split(",")[0]);
//            }
//        }
//    }
//
//    public static void main(String[] args) {
//        KafkaUDTF kafkaUDTF = new KafkaUDTF("name,VARCHAR", "age,INTEGER", "grade,VARCHAR");
//        kafkaUDTF.eval("{\"name\":\"Alice\", \"age\":13,  \"grade\":\"A\"}".getBytes());
//    }
//
//    public void eval(byte[] message) {
//        String mess = new String(message, Charset.forName("UTF-8"));
//        JSONObject json = JSON.parseObject(mess);
//        Row row = new Row(fieldName.size());
//        for (int i = 0; i < fieldName.size(); i++) {
//            row.setField(i, json.get(fieldName.get(i)));
//        }
//        collect(row);
//    }
//
//    @Override
//    // 如果返回值是Row，就必须重载实现这个方法，显式地告诉系统返回的字段类型
//    public DataType getResultType(Object[] arguments, Class[] argTypes) {
//        TypeInformation[] typeInformations = new TypeInformation[clazzes.size()];
//
//        for (int i = 0; i < clazzes.size(); i++) {
//            typeInformations[i] = BasicTypeInfo.of(clazzes.get(i));
//        }
//        RowTypeInfo rowType = new RowTypeInfo(typeInformations);
//        return new TypeInfoWrappedDataType(rowType);
//    }
//
//}