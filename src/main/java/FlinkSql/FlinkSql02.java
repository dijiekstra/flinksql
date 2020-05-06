package FlinkSql;

import static util.FlinkConstant.*;

public class FlinkSql02 {

    public static void main(String[] args) throws Exception {

        //通过DDL，注册kafka数据源表
        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL);

        //通过DDL，注册mysql数据结果表
        tEnv.sqlUpdate(MYSQL_TABLE_SINK_DDL);

        //将从kafka中查到的数据，插入mysql中
        tEnv.sqlUpdate("insert into user_behavior_mysql select user_id,item_id,behavior,category_id,ts from user_behavior limit 1");

        //任务启动，这行必不可少！
        env.execute("test");

    }
}