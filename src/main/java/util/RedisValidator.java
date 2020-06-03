package util;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

public class RedisValidator extends ConnectorDescriptorValidator {

    public static final String CONNECTOR_TYPE_VALUE_REDIS = "redis";

    public static final String CONNECTOR_IP = "connector.ip";

    public static final String CONNECTOR_PORT = "connector.port";

    public static final String CONNECTOR_VERSION = "connector.version";

    public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "connector.lookup.cache.max-rows";

    public static final String CONNECTOR_LOOKUP_CACHE_TTL = "connector.lookup.cache.ttl";


    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        validateConnectProperties(properties);
        validateCacheProperties(properties);

    }

    private void validateConnectProperties(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_IP, false, 8);
        properties.validateInt(CONNECTOR_PORT, false, 1);
        properties.validateDouble(CONNECTOR_VERSION, true, 2.6);


    }

    private void validateCacheProperties(DescriptorProperties properties) {

        properties.validateLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS, true);
        properties.validateLong(CONNECTOR_LOOKUP_CACHE_TTL, true, 1);

        checkAllOrNone(properties, new String[]{
                CONNECTOR_LOOKUP_CACHE_MAX_ROWS,
                CONNECTOR_LOOKUP_CACHE_TTL
        });
    }
    private void checkAllOrNone(DescriptorProperties properties, String[] propertyNames) {
        int presentCount = 0;
        for (String name : propertyNames) {
            if (properties.getOptionalString(name).isPresent()) {
                presentCount++;
            }
        }
        Preconditions.checkArgument(presentCount == 0 || presentCount == propertyNames.length,
                "Either all or none of the following properties should be provided:\n" + String.join("\n", propertyNames));
    }
}
