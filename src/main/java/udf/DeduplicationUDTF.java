package udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DeduplicationUDTF extends TableFunction<Tuple2<String, Integer>> {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationUDTF.class);

    private transient Connection hConnection;
    private transient Table table;
    private transient BufferedMutator mutator;

    private final String zkIp;
    private final String zkPort;

    private final String tableName;
    private final String cf;
    private final String col;

    public DeduplicationUDTF(String zkIp, String zkPort, String tableName, String cf, String col) {
        this.zkIp = zkIp;
        this.zkPort = zkPort;
        this.tableName = tableName;
        this.cf = cf;
        this.col = col;
    }

    public void eval(String rowkey) {
        Get get = new Get(Bytes.toBytes(rowkey));


        try {
            Result result = table.get(get);

            //说明hbase中已有数据，这条可以过滤，所以给这条数据JOIN的字段is_duplicate赋值1
            if (!result.isEmpty()) {

                collect(Tuple2.of(rowkey, 1));

            } else {

                //没有的话，就把这条数据放到hbase，然后再这条数据JOIN的字段is_duplicate赋值1，最后下发
                //注意，必须先入hbase，而且hbase连接得是同步的，不然会有数据还没插入hbase，又有新数据到了的情况
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes("1"));

                mutator.mutate(put);
                mutator.flush();

                collect(Tuple2.of(rowkey, -1));

            }


        } catch (IOException e) {
            log.error("get from hbase error! ", e);
            e.printStackTrace();
        }

    }

    @Override
    public void open(FunctionContext context) throws Exception {

        super.open(context);
        //初始化hbase 连接
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkIp);
        config.set("hbase.zookeeper.property.clientPort", zkPort);

        hConnection = ConnectionFactory.createConnection(config);

        table = hConnection.getTable(TableName.valueOf(tableName));
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                .writeBufferSize(-1);
        mutator = hConnection.getBufferedMutator(params);

    }

    @Override
    public void close() {
        //所有的流都关闭
        try {
            super.close();
        } catch (Exception e) {
            log.error("super class close error!", e);
            throw new RuntimeException(e);
        }

        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                log.error("table close error!", e);
                throw new RuntimeException(e);
            }
        }

        if (mutator != null) {
            try {
                mutator.close();
            } catch (IOException e) {
                log.error("mutator close error!", e);
                throw new RuntimeException(e);
            }
        }

        if (hConnection != null) {
            try {
                hConnection.close();
            } catch (IOException e) {
                log.error("Connection close error!", e);
                throw new RuntimeException(e);
            }
        }

    }
}
