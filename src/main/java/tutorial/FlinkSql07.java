package tutorial;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import static util.FlinkConstant.*;

public class FlinkSql07 {

    //DDL语句
    public static final String REDIS_TABLE_DIM_DDL = "" +
            "CREATE TABLE redis_dim (\n" +
            "first String,\n" +
            "name String\n" +
            ") WITH (\n" +
            "  'connector.type' = 'redis',  \n" +
            "  'connector.ip' = '127.0.0.1', \n" +
            "  'connector.port' = '6379', \n" +
            "  'connector.lookup.cache.max-rows' = '10', \n" +
            "  'connector.lookup.cache.ttl' = '10000000', \n" +
            "  'connector.version' = '2.6' \n" +
            ")";

    public static void main(String[] args) throws Exception {

        DataStream<Row> ds = env.addSource(new RichParallelSourceFunction<Row>() {

            volatile boolean flag = true;

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (flag) {
                    Row row = new Row(2);
                    row.setField(0, 1);
                    row.setField(1, "a");
                    ctx.collect(row);
                    Thread.sleep(1000);
                }

            }

            @Override
            public void cancel() {
                flag = false;
            }
        }).returns(Types.ROW(Types.INT, Types.STRING));

        //注册redis维表
        tEnv.sqlUpdate(REDIS_TABLE_DIM_DDL);

        //source注册成表
        tEnv.createTemporaryView("test", ds, "id,first,p.proctime");

        //join语句
        Table table = tEnv.sqlQuery("select a.*,b.* from test a left join redis_dim FOR SYSTEM_TIME AS OF a.p AS b on a.first = b.first");

        //输出
        tEnv.toAppendStream(table, Row.class).print("FlinkSql07");

        env.execute("FlinkSql07");


    }
}
