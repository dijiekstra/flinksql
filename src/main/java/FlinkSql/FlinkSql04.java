package FlinkSql;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import udf.TestAggregateFunction;
import udf.TestScalarFunc;
import udf.TestTableAggregateFunction;
import udf.TestTableFunction;


public class FlinkSql04 {
    public static void main(String[] args) throws Exception {

        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        DataStream<Row> source = env.addSource(new RichSourceFunction<Row>() {

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                    Row row = new Row(3);
                    row.setField(0, 2);
                    row.setField(1, 3);
                    row.setField(2, 3);
                    ctx.collect(row);
            }

            @Override
            public void cancel() {

            }
        }).returns(Types.ROW(Types.INT,Types.INT,Types.INT));

        tEnv.createTemporaryView("t",source,"a,b,c");

//        tEnv.sqlUpdate("CREATE FUNCTION IF NOT EXISTS test AS 'udf.TestScalarFunc'");

        tEnv.registerFunction("test",new TestScalarFunc());

        Table table = tEnv.sqlQuery("select test() as a,test(a) as b, test(a,b,c) as c from t");

        DataStream<Row> res = tEnv.toAppendStream(table, Row.class);

//        res.print().name("Scalar Functions Print").setParallelism(1);

        DataStream<Row> ds2 = env.addSource(new RichSourceFunction<Row>() {

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                    Row row = new Row(2);
                    row.setField(0, 22);
                    row.setField(1, "aa,b,cdd,dfsfdg,exxxxx");
                    ctx.collect(row);
            }

            @Override
            public void cancel() {

            }
        }).returns(Types.ROW(Types.INT, Types.STRING));

        tEnv.createTemporaryView("t2",ds2,"age,name_list");

        tEnv.registerFunction("test2",new TestTableFunction(","));

//        Table table2 = tEnv.sqlQuery("select a.age,b.name,b.name_length from t2 a, LATERAL TABLE(test2(a.name_list)) as b(name, name_length)");

        Table table2 = tEnv.sqlQuery("select a.age,b.name,b.name_length from t2 a LEFT JOIN LATERAL TABLE(test2(a.name_list)) as b(name, name_length) ON TRUE");

        DataStream<Row> res2 = tEnv.toAppendStream(table2, Row.class);

//        res2.print().name("Table Functions Print").setParallelism(1);

        DataStream<Row> ds3 = env.addSource(new RichSourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                Row row1 = new Row(2);
                row1.setField(0,"a");
                row1.setField(1,1L);

                Row row2 = new Row(2);
                row2.setField(0,"a");
                row2.setField(1,2L);

                Row row3 = new Row(2);
                row3.setField(0,"b");
                row3.setField(1,100L);

                ctx.collect(row1);
                ctx.collect(row2);
                ctx.collect(row3);

            }

            @Override
            public void cancel() {

            }
        }).returns(Types.ROW(Types.STRING, Types.LONG));

        tEnv.createTemporaryView("t3",ds3,"name,cnt");

        tEnv.registerFunction("test3",new TestAggregateFunction());

        Table table3 = tEnv.sqlQuery("select name,test3(cnt) as mySum from t3 group by name");

        DataStream<Tuple2<Boolean, Row>> res3 = tEnv.toRetractStream(table3, Row.class);

//        res3.print().name("Aggregate Functions Print").setParallelism(1);

        DataStream<Row> ds4 = env.addSource(new RichSourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                Row row1 = new Row(2);
                row1.setField(0,"a");
                row1.setField(1,1);

                Row row2 = new Row(2);
                row2.setField(0,"a");
                row2.setField(1,2);

                Row row3 = new Row(2);
                row3.setField(0,"a");
                row3.setField(1,100);

                ctx.collect(row1);
                ctx.collect(row2);
                ctx.collect(row3);
            }

            @Override
            public void cancel() {

            }
        }).returns(Types.ROW(Types.STRING, Types.INT));

        tEnv.createTemporaryView("t4",ds4,"name,cnt");

        tEnv.registerFunction("test4",new TestTableAggregateFunction());

        Table table4 = tEnv.sqlQuery("select * from t4");

        Table table5 = table4.groupBy("name")
                .flatAggregate("test4(cnt) as (v,rank)")
                .select("name,v,rank");

        DataStream<Tuple2<Boolean, Row>> res4 = tEnv.toRetractStream(table5, Row.class);

        res4.print().name("Table Aggregate Functions Print").setParallelism(1);

        env.execute("test udf");

    }
}
