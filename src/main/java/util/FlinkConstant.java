package util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkConstant {
    public static final String KAFKA_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE user_behavior (\n" +
            "    user_id BIGINT,\n" +
            "    item_id BIGINT,\n" +
            "    category_id BIGINT,\n" +
            "    behavior STRING,\n" +
            "    ts TIMESTAMP(3)\n" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = 'mykafka3', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'earliest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'json'  -- json格式，和topic中的消息格式保持一致\n" +
            ")";

    public static final String MYSQL_TABLE_SINK_DDL = "" +
            "CREATE TABLE `user_behavior_mysql` (\n" +
            "  `user_id` bigint  ,\n" +
            "  `item_id` bigint  ,\n" +
            "  `behavior` varchar  ,\n" +
            "  `category_id` bigint  ,\n" +
            "  `ts` timestamp(3)   \n" +
            ")WITH (\n" +
            "  'connector.type' = 'jdbc', -- 连接方式\n" +
            "  'connector.url' = 'jdbc:mysql://localhost:3306/test', -- jdbc的url\n" +
            "  'connector.table' = 'user_behavior',  -- 表名\n" +
            "  'connector.driver' = 'com.mysql.jdbc.Driver', -- 驱动名字，可以不填，会自动从上面的jdbc url解析 \n" +
            "  'connector.username' = 'root', -- 顾名思义 用户名\n" +
            "  'connector.password' = '123456' , -- 密码\n" +
            "  'connector.write.flush.max-rows' = '5000', -- 意思是攒满多少条才触发写入 \n" +
            "  'connector.write.flush.interval' = '2s' -- 意思是攒满多少秒才触发写入；这2个参数，无论数据满足哪个条件，就会触发写入\n" +
            ")";

    public static final String CSV_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE csv_source (\n" +
            " user_id bigint,\n" +
            " item_id bigint,\n" +
            " category_id bigint,\n" +
            " behavior varchar,\n" +
            " ts bigint,\n" +
            " proctime as PROCTIME() \n"+
            ") WITH (\n" +
            " 'connector.type' = 'filesystem', -- 指定连接类型\n" +
            " 'connector.path' = 'C:\\Users\\tzmaj\\Desktop\\教程\\3\\UserBehavior.csv',-- 目录 \n" +
            " 'format.type' = 'csv', -- 文件格式 \n" +
            " 'format.field-delimiter' = ',' ,-- 字段分隔符 \n" +
            " 'format.fields.0.name' = 'user_id',-- 第N字段名，相当于表的schema，索引从0开始 \n" +
            " 'format.fields.0.data-type' = 'bigint',-- 字段类型\n" +
            " 'format.fields.1.name' = 'item_id', \n" +
            " 'format.fields.1.data-type' = 'bigint',\n" +
            " 'format.fields.2.name' = 'category_id',\n" +
            " 'format.fields.2.data-type' = 'bigint',\n" +
            " 'format.fields.3.name' = 'behavior', \n" +
            " 'format.fields.3.data-type' = 'String',\n" +
            " 'format.fields.4.name' = 'ts', \n" +
            " 'format.fields.4.data-type' = 'bigint'\n" +
            ")      ";

    public static final String KAFKA_TABLE_SINK_DDL = "" +
            " CREATE TABLE kafka_sink (\n" +
            " user_id bigint,\n" +
            " item_id bigint,\n" +
            " category_id bigint,\n" +
            " behavior varchar,\n" +
            " ts TIMESTAMP(3)\n" +
            ") WITH (\n" +
            " 'connector.type' = 'kafka',\n" +
            " 'connector.version' = '0.11',\n" +
            " 'connector.topic' = 'mykafka3',\n" +
            " 'connector.properties.zookeeper.connect' = '172.17.47.44:2181', \n" +
            " 'connector.properties.bootstrap.servers' = '172.17.47.44:9092', \n" +
            " 'update-mode' = 'append',\n" +
            " 'connector.sink-partitioner' = 'round-robin',\n" +
            " 'format.type' = 'json'\n" +
            ")";
    public static final String MYSQL_TABLE_DIM_DDL = ""+
            "CREATE TABLE `dim_behavior` (\n" +
            "  `id` int  ,\n" +
            "  `c_name` varchar  ,\n" +
            "  `e_name` varchar  \n" +
            ")WITH (\n" +
            "  'connector.type' = 'jdbc', -- 连接方式\n" +
            "  'connector.url' = 'jdbc:mysql://localhost:3306/test', -- jdbc的url\n" +
            "  'connector.table' = 'dim_behavior',  -- 表名\n" +
            "  'connector.driver' = 'com.mysql.jdbc.Driver', -- 驱动名字，可以不填，会自动从上面的jdbc url解析 \n" +
            "  'connector.username' = 'root', -- 顾名思义 用户名\n" +
            "  'connector.password' = '123456' , -- 密码\n" +
            "  'connector.lookup.cache.max-rows' = '5000', -- 缓存条数 \n"+
            "  'connector.lookup.cache.ttl' = '10s' -- 缓存时间 \n"+
            ")";

    public static final String MYSQL_TABLE_SINK_DDL2=""+
            "CREATE TABLE `result_1` (\n" +
            "  `behavior` varchar  ,\n" +
            "  `category_id` bigint  \n" +
            ")WITH (\n" +
            "  'update-mode' = 'upsert' "+
            "  'connector.type' = 'jdbc', -- 连接方式\n" +
            "  'connector.url' = 'jdbc:mysql://localhost:3306/test', -- jdbc的url\n" +
            "  'connector.table' = 'result_1',  -- 表名\n" +
            "  'connector.driver' = 'com.mysql.jdbc.Driver', -- 驱动名字，可以不填，会自动从上面的jdbc url解析 \n" +
            "  'connector.username' = 'root', -- 顾名思义 用户名\n" +
            "  'connector.password' = '123456' , -- 密码\n" +
            "  'connector.write.flush.max-rows' = '5000', -- 意思是攒满多少条才触发写入 \n" +
            "  'connector.write.flush.interval' = '1' -- 意思是攒满多少秒才触发写入；这2个参数，无论数据满足哪个条件，就会触发写入\n" +
            ")";


    //构建StreamExecutionEnvironment
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //构建EnvironmentSettings 并指定Blink Planner
    private static final EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

    //构建StreamTableEnvironment
    public static final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);


}
