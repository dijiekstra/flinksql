package source;

import lookup.RedisLookupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class RedisLookupTableSource implements
        LookupableTableSource<Row>, StreamTableSource<Row> {

    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String ip;
    private final int port;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    private RedisLookupTableSource(String[] fieldNames, TypeInformation[] fieldTypes, String ip, int port, long cacheMaxSize, long cacheExpireMs) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.ip = ip;
        this.port = port;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    //返回同步的，我们用的是异步的，这边直接返回null
    @Override
    public TableFunction getLookupFunction(String[] lookupKeys) {
        return null;
    }

    //返回异步的
    @Override
    public AsyncTableFunction getAsyncLookupFunction(String[] lookupKeys) {
        return RedisLookupFunction.builder()
                .setFieldNames(fieldNames)
                .setFieldTypes(fieldTypes)
                .setIp(ip)
                .setPort(port)
                .setCacheMaxSize(cacheMaxSize)
                .setCacheExpireMs(cacheExpireMs)
                .build();
    }

    //表示是异步
    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

    //表结构
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
                .build();
    }

    //做数据源的时候使用，我们这用不上，直接报错了
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        throw new IllegalArgumentException("unSupport source now");
    }

    //数据输出结构
    @Override
    public DataType getProducedDataType() {
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames));
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private String ip;
        private int port;

        private long cacheMaxSize;
        private long cacheExpireMs;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        public RedisLookupTableSource build() {
            return new RedisLookupTableSource(fieldNames, fieldTypes, ip, port, cacheMaxSize, cacheExpireMs);
        }
    }
}
