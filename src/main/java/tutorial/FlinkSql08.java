package tutorial;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import udf.DeduplicationUDTF;

import static util.FlinkConstant.*;

public class FlinkSql08 {

    public static final String KAFKA_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE t1 (\n" +
            "    user_id BIGINT,\n" +
            "    item_id BIGINT,\n" +
            "    category_id BIGINT,\n" +
            "    behavior STRING,\n" +
            "    ts BIGINT,\n" +
            "    p AS PROCTIME()" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = '08_test', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = '08_test', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'earliest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'json'  -- json格式，和topic中的消息格式保持一致\n" +
            ")";

    public static final String MYSQL_TABLE_SINK_DDL = "" +
            "CREATE TABLE `t2` (\n" +
            "  `user_id` BIGINT  ,\n" +
            "  `item_id` BIGINT  ,\n" +
            "  `behavior` STRING  ,\n" +
            "  `category_id` BIGINT  ,\n" +
            "  `ts` BIGINT   \n" +
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

    public static final String ES_TABLE_SINK_DDL = "" +
            "CREATE TABLE `t3` (\n" +
            "  `user_id` BIGINT  ,\n" +
            "  `item_id` BIGINT  ,\n" +
            "  `behavior` STRING  ,\n" +
            "  `category_id` BIGINT  ,\n" +
            "  `ts` BIGINT   \n" +
            ")WITH (\n" +
            "  'connector.type' = 'elasticsearch', -- required: specify this table type is elasticsearch\n" +
            "  'connector.version' = '6',          -- required: valid connector versions are \"6\"\n" +
            "  'connector.hosts' = 'http://127.0.0.1:9200',  -- required: one or more Elasticsearch hosts to connect to\n" +
            "  'connector.index' = 'user',       -- required: Elasticsearch index\n" +
            "  'connector.document-type' = 'user',  -- required: Elasticsearch document type\n" +
            "  'update-mode' = 'upsert',            -- optional: update mode when used as table sink.           \n" +
            "  'connector.flush-on-checkpoint' = 'false',   -- optional: disables flushing on checkpoint (see notes below!)\n" +
            "  'connector.bulk-flush.max-actions' = '1',  -- optional: maximum number of actions to buffer \n" +
            "  'connector.bulk-flush.max-size' = '1 mb',  -- optional: maximum size of buffered actions in bytes\n" +
            "  'connector.bulk-flush.interval' = '1000',  -- optional: bulk flush interval (in milliseconds)\n" +
            "  'connector.bulk-flush.backoff.max-retries' = '3',  -- optional: maximum number of retries\n" +
            "  'connector.bulk-flush.backoff.delay' = '1000',    -- optional: delay between each backoff attempt\n" +
            "  'format.type' = 'json'   -- required: Elasticsearch connector requires to specify a format,\n" +
            ")";

    public static void main(String[] args) throws Exception {

        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL);

        tEnv.sqlUpdate(MYSQL_TABLE_SINK_DDL);

        tEnv.sqlUpdate(ES_TABLE_SINK_DDL);

//        String topN_lastRow = "insert into t2 " +
//                "select user_id ,item_id ,behavior ,category_id ,ts from " +
//                " (select *,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY p DESC ) AS rn from t1) " +
//                " where rn = 1";
//
//        tEnv.sqlUpdate(topN_lastRow);

//        String topN_firstRow = "insert into t2 " +
//                "select user_id ,item_id ,behavior ,category_id ,ts from " +
//                " (select *,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY p ASC ) AS rn from t1) " +
//                " where rn = 1";
//
//        tEnv.sqlUpdate(topN_firstRow);

//        String groupBy_lastRow = "insert into t3 \n" +
//                "select \n" +
//                " user_id \n" +
//                ",last_value(item_id) as item_id \n" +
//                ",last_value(behavior) as behavior \n" +
//                ",last_value(category_id) as category_id \n" +
//                ",last_value(ts) as ts \n" +
//                "from t1 group by user_id";
//
//        tEnv.sqlUpdate(groupBy_lastRow);

//        String groupBy_firstRow = "insert into t3 \n" +
//                "select \n" +
//                " user_id \n" +
//                ",first_value(item_id) as item_id \n" +
//                ",first_value(behavior) as behavior \n" +
//                ",first_value(category_id) as category_id \n" +
//                ",first_value(ts) as ts \n" +
//                "from t1 group by user_id";
//
//        tEnv.sqlUpdate(groupBy_firstRow);

        tEnv.registerFunction("deDuplication",new DeduplicationUDTF("127.0.0.1","2182","test","cf","col"));

        //给每条数据打上标签，is_duplicate为1的表示为重复，-1表示没有重复，也就是第一条到达的数据
        Table table = tEnv.sqlQuery("select a.* ,b.* from t1 a , \n" +
                "LATERAL TABLE(deDuplication(concat_ws('',cast(a.user_id as varchar)))) as b(rowkey,is_duplicate)");

        tEnv.toAppendStream(table,Row.class).print("没用where过滤").setParallelism(1);

        Table where = table.where("is_duplicate = -1");

        //这个应该只会输出10条数据，而且主键user_id都是唯一，否则就有误
        //大家多次测试的时候记得删除HBASE中的数据
        tEnv.toAppendStream(where,Row.class).print("用where过滤").setParallelism(1);


        env.execute("FlinkSql08");

    }
}
