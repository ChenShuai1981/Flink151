import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.PrintTableSink;

import java.util.TimeZone;

public class KafkaUDTFTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataStreamSource<byte[]> byteSource = env.fromElements("{\"name\": \"Alice\",\"age\": 13,\"grade\": \"A\",\"scores\": [89, 68, 94]}".getBytes());
        Table byteSourceTable = tableEnv.fromDataStream(byteSource, "message");

        tableEnv.registerTable("b", byteSourceTable);
        tableEnv.registerFunction("kafkaUDTF", new KafkaUDTF("name,VARCHAR", "age,INTEGER", "grade,VARCHAR", "scores,ARRAY"));

        Table res1 = tableEnv.sqlQuery("select  T.name, T.age, T.grade, T.scores\n" +
                "from b as S\n" +
                "LEFT JOIN LATERAL TABLE(kafkaUDTF(message)) as T(name, age, grade, scores) ON TRUE");
        res1.writeToSink(new PrintTableSink(TimeZone.getDefault()));
        tableEnv.execute();
    }
}
