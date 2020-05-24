package FlinkSql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class FlinkSql05 {

    public static final String KAFKA_TABLE_SOURCE_DDL_01 = ""+
            "CREATE TABLE t1 (\n" +
            "    user_id BIGINT,\n" +
            "    order_id BIGINT,\n" +
            "    ts BIGINT\n" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = 'unBoundedJoin01_t1', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'latest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'csv'  -- csv格式，和topic中的消息格式保持一致\n" +
            ")";

    public static final String KAFKA_TABLE_SOURCE_DDL_02 = ""+
            "CREATE TABLE t2 (\n" +
            "    order_id BIGINT,\n" +
            "    item_id BIGINT,\n" +
            "    ts BIGINT\n" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = 'unBoundedJoin01_t2', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'latest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'csv'  -- csv格式，和topic中的消息格式保持一致\n" +
            ")";
    public static final String KAFKA_TABLE_SOURCE_DDL_03 = ""+
            "CREATE TABLE t3 (\n" +
            "    user_id BIGINT,\n" +
            "    order_id BIGINT,\n" +
            "    ts BIGINT,\n" +
            "    r_t AS TO_TIMESTAMP(FROM_UNIXTIME(ts,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),-- 计算列，因为ts是bigint，没法作为水印，所以用UDF转成TimeStamp\n"+
            "    WATERMARK FOR r_t AS r_t - INTERVAL '5' SECOND -- 指定水印生成方式\n"+
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = 'timeIntervalJoin_01', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'latest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'csv'  -- csv格式，和topic中的消息格式保持一致\n" +
            ")";

    public static final String KAFKA_TABLE_SOURCE_DDL_04 = ""+
            "CREATE TABLE t4 (\n" +
            "    order_id BIGINT,\n" +
            "    item_id BIGINT,\n" +
            "    ts BIGINT,\n" +
            "    r_t AS TO_TIMESTAMP(FROM_UNIXTIME(ts,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),-- 计算列，因为ts是bigint，没法作为水印，所以用UDF转成TimeStamp\n"+
            "    WATERMARK FOR r_t AS r_t - INTERVAL '5' SECOND -- 指定水印生成方式\n"+
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = 'timeIntervalJoin_02', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'latest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'csv'  -- csv格式，和topic中的消息格式保持一致\n" +
            ")";


//    public static final String MYSQL_TABLE_SINK = "";

    public static void main(String argv[]) throws Exception {

        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        //注册kafka 数据源表
        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL_01);

        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL_02);

        //左表数据  543462,1001,1511658000
        //右表数据  1001,4238,1511658001
        //不用一开始就给kafka灌入数据，可以等任务正常启动没有数据后再输入数据，方便观察现象

        //UnBounded 双流Join 之 Inner Join
        Table unBoundedJoin_inner_join = tEnv.sqlQuery("select a.*,b.* from t1 a inner join t2 b on a.order_id = b.order_id");

        DataStream<Tuple2<Boolean, Row>> unBoundedJoin_inner_join_DS = tEnv.toRetractStream(unBoundedJoin_inner_join, Row.class);

        //在一开始没有数据时，没有输出；当我们往左表的kafka中输入543462,1001,1511658000时，依旧没有数据下发，符合我们之前所说的言论
        //之后再往右表灌入数据，此时会有数据输出
        //(true,543462,1001,1511658000,1001,4238,1511658001)
//        unBoundedJoin_inner_join_DS.print().setParallelism(1).name("unBoundedJoin_inner_join");

        //UnBounded 双流Join 之 Left Join
        //再准备几条kafka数据
        //左表    223813,2042400,1511658002
        //右表    2042400,4104826,1511658001
        //同样也是先别灌入

        Table unBoundedJoin_left_join = tEnv.sqlQuery("select a.*,b.* from t1 a left join t2 b on a.order_id = b.order_id");

        DataStream<Tuple2<Boolean, Row>> unBoundedJoin_left_join_DS = tEnv.toRetractStream(unBoundedJoin_left_join, Row.class);

//        unBoundedJoin_left_join_DS.print().setParallelism(1).name("unBoundedJoin_left_join");
        //此时左表输入223813,2042400,1511658002，发现数据下发，右边都为NULL
        //输出：(true,223813,2042400,1511658002,null,null,null)
        //然后再将2042400,4104826,1511658001插入右表中
        //(false,223813,2042400,1511658002,null,null,null)
        //(true,223813,2042400,1511658002,2042400,4104826,1511658001)
        //与我们前面所说一致！先是输出右边补齐为NULL的数据，等能够Join上了，再撤回刚才的数据，重新将Join之后的数据下发
        //我们测试的都是左表先到，而右表在等待的情况，那么如果右表先到，左表后到，数据结果又是什么样呢？大家自行尝试吧



        //Time Interval 双流JOIN

        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL_03);

        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL_04);

        //左表数据  543462,1001,1511658000
        //右表数据  1001,4238,1511658011

        //使用time interval join，并且指定时间范围为t3.r_t的上下10秒内
        Table timeIntervalJoin = tEnv.sqlQuery(""+
                "select t3.*,t4.item_id,t4.ts from t3 join t4 on t3.order_id = t4.order_id " +
                "and t4.r_t between t3.r_t - interval '10' second and t3.r_t + interval '10' second ");

        //因为是time interval join，所以不会有撤回事件发生，所以使用append流
        DataStream<Row> tiemIntervalJoinDs = tEnv.toAppendStream(timeIntervalJoin, Row.class);

        tiemIntervalJoinDs.print().setParallelism(1).name("timeIntervalJoin");
        //当我们将数据输入各自的kafka topic中后，发现并没有数据输出，因为t3.r_t - t4.r_t = -11，已经超过了我们指定的时间范围
        //右表再输入1001,4238,1511658010
        //输出：543462,1001,1511658000,2017-11-26T09:00,4238,1511658010
        //time interval join之后可以再接窗口计算，这里就不给大家实际演示了，大家自行操作吧

        //执行任务，必不可少一句话！
        env.execute("双流join");
    }
}
