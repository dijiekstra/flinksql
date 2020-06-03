package tutorial;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


public class FlinkSql03 {

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

    public static final String MYSQL_TABLE_SINK_DDL=""+
            "CREATE TABLE `result_1` (\n" +
            "  `behavior` varchar  ,\n" +
            "  `count_unique_user` bigint,  \n" +
            "  `e_name` varchar  \n" +
            ")WITH (\n" +
            "  'connector.type' = 'jdbc', -- 连接方式\n" +
            "  'connector.url' = 'jdbc:mysql://localhost:3306/test', -- jdbc的url\n" +
            "  'connector.table' = 'result_1',  -- 表名\n" +
            "  'connector.driver' = 'com.mysql.jdbc.Driver', -- 驱动名字，可以不填，会自动从上面的jdbc url解析 \n" +
            "  'connector.username' = 'root', -- 顾名思义 用户名\n" +
            "  'connector.password' = '123456' , -- 密码\n" +
            "  'connector.write.flush.max-rows' = '5000', -- 意思是攒满多少条才触发写入 \n" +
            "  'connector.write.flush.interval' = '1' -- 意思是攒满多少秒才触发写入；这2个参数，无论数据满足哪个条件，就会触发写入\n" +
//            "  'update-mode' = 'upsert' -- 指定为插入更新模式 \n"+
            ")";

    public static void main(String[] args) throws Exception {

        //构建StreamExecutionEnvironment
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //构建EnvironmentSettings 并指定Blink Planner
         EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //构建StreamTableEnvironment
         StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        //注册csv文件数据源表
        tEnv.sqlUpdate(CSV_TABLE_SOURCE_DDL);

        //注册mysql数据维表
        tEnv.sqlUpdate(MYSQL_TABLE_DIM_DDL);

        //注册mysql数据结果表
        tEnv.sqlUpdate(MYSQL_TABLE_SINK_DDL);

        //计算每种类型的行为有多少用户
        Table group = tEnv.sqlQuery("select behavior,count(distinct user_id) count_unique_user from csv_source group by behavior ");

        //转回datastream，因为需要增加proctime，而目前定义proctime方式只有两种，一种是在定义DDL的时候，一种是在DataStream转 Table的时候
        //转撤回流是因为上面的sql用了group by，所以只能使用撤回流
        DataStream<Row> ds = tEnv.toRetractStream(group, Row.class).flatMap(
                new FlatMapFunction<Tuple2<Boolean, Row>, Row>() {
                    @Override
                    public void flatMap(Tuple2<Boolean, Row> value, Collector<Row> collect) throws Exception {

                        collect.collect(value.f1);
                    }
                }
        ).returns(Types.ROW(Types.STRING,Types.LONG));

        //给Table增加proctime字段，ts可以随便改成别的你喜欢的名字
        Table table = tEnv.fromDataStream(ds, "behavior,count_unique_user,ts.proctime");

        //建立视图，保留临时表
        tEnv.createTemporaryView("group_by_view",table);

        //pv，buy，cart...等行为对应的英文名，我们通过维表Join的方式，替换为中文名
        //FOR SYSTEM_TIME AS OF a.ts AS b 这是固定写法，ts与上面指定dataStream schema时候用的名字一致
        //这里之所以再group by，是让这次查询变成撤回流，这样插入mysql时，可以通过主键自动update数据
        Table join = tEnv.sqlQuery("select b.c_name as behavior , max(a.count_unique_user) ,a.behavior as e_name " +
                "from group_by_view a " +
                "left join dim_behavior FOR SYSTEM_TIME AS OF a.ts AS b " +
                "on a.behavior = b.e_name " +
                "group by a.behavior,b.c_name");

        //建立视图，保留临时表
        tEnv.createTemporaryView("join_view",join);

        //数据输出到mysql
        tEnv.sqlUpdate("insert into result_1 select * from join_view");

        //任务启动，这行必不可少！
        env.execute("FlinkSql03");
    }

}
