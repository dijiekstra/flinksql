package lookup;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RedisLookupFunction extends AsyncTableFunction<Row> {

    private static final Logger log = LoggerFactory.getLogger(RedisLookupFunction.class);


    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String ip;
    private final int port;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private transient Cache<String, String> cache;


    //异步客户端
    private transient RedisAsyncCommands<String, String> asyncClient;
    private transient RedisClient redisClient;


    public RedisLookupFunction(String[] fieldNames, TypeInformation[] fieldTypes, String ip, int port, long cacheMaxSize, long cacheExpireMs) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.ip = ip;
        this.port = port;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void eval(CompletableFuture<Collection<Row>> future, String key) {

        //先去缓存取，取的到就返回，取不到就从redis拿
        if (cache != null) {

            String value = cache.getIfPresent(key);
            log.info("value in cache is null?:{}", value == null);
            if (value != null) {
                future.complete(Collections.singletonList(Row.of(key, value)));
                return;
            }
        }

        //异步从redis中获取，如果redis取出为null，赋值""，防止这条key下次再来又查redis，导致缓存雪崩
        try {
            asyncClient.get(key).thenAccept(value -> {
                if (value == null) {
                    value = "";
                }
                if (cache != null)
                    cache.put(key, value);
                future.complete(Collections.singletonList(Row.of(key, value)));
            });
        } catch (Exception e) {
            log.error("get from redis fail", e);
            throw new RuntimeException("get from redis fail", e);
        }


    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            //建立redis 异步客户端，我这用的是 Lettuce 也可以使用别的，随意
            RedisURI redisUri = RedisURI.builder()
                    .withHost(ip)
                    .withPort(port)
                    .build();
            redisClient = RedisClient.create(redisUri);
            asyncClient = redisClient.connect().async();
        } catch (Exception e) {
            throw new Exception("build redis async client fail", e);
        }

        try {
            //初始化缓存大小
            this.cache = cacheMaxSize <= 0 || cacheExpireMs <= 0 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
            log.info("cache is null ? :{}", cache == null);
        } catch (Exception e) {
            throw new Exception("build cache fail", e);

        }


    }

    //返回类型
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    //扫尾工作，关闭连接
    @Override
    public void close() throws Exception {
        log.error("shutdown");
        cache.cleanUp();
        redisClient.shutdown();
        super.close();
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


        public RedisLookupFunction build() {
            return new RedisLookupFunction(fieldNames, fieldTypes, ip, port, cacheMaxSize, cacheExpireMs);
        }
    }
}
