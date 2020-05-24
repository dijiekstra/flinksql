package FlinkSql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class FlinkSql05 {

    public static final String KAFKA_TABLE_SOURCE_01 = "";
    public static final String KAFKA_TABLE_SOURCE_02 = "";

    public static final String MYSQL_TABLE_SINK = "";

    public static void main(String argv[]) throws Exception {

        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_01);

        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_02);

        //UnBounded 双流join
        Table unBoundedJoin = tEnv.sqlQuery("");

        DataStream<Tuple2<Boolean, Row>> unBoundedJoinDS = tEnv.toRetractStream(unBoundedJoin, Row.class);

        unBoundedJoinDS.print().setParallelism(1).name("unBoundedJoin");

        //Time Interval 双流JOIN
//        Table timeIntervalJoin = tEnv.sqlQuery("");
//
//        DataStream<Tuple2<Boolean, Row>> tiemIntervalJoinDs = tEnv.toRetractStream(timeIntervalJoin, Row.class);
//
//        tiemIntervalJoinDs.print().setParallelism(1).name("timeIntervalJoin");
//
//        env.execute("双流join");
    }
}
