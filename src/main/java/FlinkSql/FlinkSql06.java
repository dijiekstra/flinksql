package FlinkSql;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import static util.FlinkConstant.env;
import static util.FlinkConstant.tEnv;

public class FlinkSql06 {

    public static final String KAFKA_TABLE_SOURCE_DDL_01 = "" +
            "CREATE TABLE t1 ( \n" +
            "afterColumns ROW(created STRING,extra ROW(canGiving BOOLEAN),`parameter` ARRAY <INT>) ,\n" +
            "beforeColumns STRING ,\n" +
            "tableVersion ROW(binlogFile STRING,binlogPosition INT ,version INT) ,\n" +
            "p AS PROCTIME(),\n"+
            "touchTime bigint \n" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = 'json_parse', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'earliest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'json'  -- json格式，和topic中的消息格式保持一致\n" +
            ")";

    public static final String HBASE_TABLE_DIM_DDL = ""+
            "CREATE TABLE t2 (\n" +
            "rowkey String,\n" +
            "f1 ROW<col1 String>\n" +
            ") WITH (\n" +
            "  'connector.type' = 'hbase', -- required: specify this table type is hbase\n" +
            "  'connector.version' = '1.4.3',          -- required: valid connector versions are \"1.4.3\"\n" +
            "  'connector.table-name' = 't2',  -- required: hbase table name\n" +
            "  'connector.zookeeper.quorum' = 'localhost:2182', -- required: HBase Zookeeper quorum configuration\n" +
            "  'connector.zookeeper.znode.parent' = '/hbase'    -- optional: the root dir in Zookeeper for HBase cluster.\n" +
            ")";

    public static void main(String[] args) throws Exception {
        //{"afterColumns":{"created":"1589186680","extra":{"canGiving":false},"parameter":[1,2,3,4]},"beforeColumns":null,"tableVersion":{"binlogFile":null,"binlogPosition":0,"version":0},"touchTime":1589186680591}

        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL_01);

        tEnv.sqlUpdate(HBASE_TABLE_DIM_DDL);

        Table table = tEnv.sqlQuery("select a.* ,b.* from t1 a left join  t2 FOR SYSTEM_TIME AS OF a.p AS b on a.afterColumns.created = b.rowkey");

        Table table2 = tEnv.sqlQuery("select afterColumns.created from t1");

        table.printSchema();

        tEnv.toRetractStream(table, Row.class).print();
        tEnv.toRetractStream(table2, Row.class).print();

        env.execute("Flink Sql 06");
    }
}
