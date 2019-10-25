import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.PrintTableSink;

import java.util.TimeZone;


public class ParseStringUdtfTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
        DataStreamSource<byte[]> byteSource = env.fromElements("{\"name\": \"Alice\",\"age\": 13,\"adult\": false,\"scores\": [89, 68, 94],\"colors\": [\"red\",\"white\"]}".getBytes());
        Table byteSourceTable = tableEnv.fromDataStream(byteSource, "message");
        tableEnv.registerTable("b", byteSourceTable);
        TableFunction stringParseFunc = new ParseStringUdtf("PersonParser");
        tableEnv.registerFunction("string_parse", stringParseFunc);
        Table res1 = tableEnv.sqlQuery("select  T.name, T.age, T.grade, T.scores, T.colors\n" +
                "from b as S\n" +
                "LEFT JOIN LATERAL TABLE(string_parse(message)) as T(name, age, grade, scores, colors) ON TRUE");
        res1.writeToSink(new PrintTableSink(TimeZone.getDefault()));
        env.execute("test udtf");
    }
}
