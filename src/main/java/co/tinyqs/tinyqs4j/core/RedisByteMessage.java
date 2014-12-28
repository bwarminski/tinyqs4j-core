package co.tinyqs.tinyqs4j.core;

import java.util.Collections;
import java.util.Map;

import co.tinyqs.tinyqs4j.api.ByteMessage;

/**
 * @author bwarminski
 *
 */
public class RedisByteMessage implements ByteMessage
{
    private final String uuid;
    private final byte[] bytes;
    private final long expiration;
    private final int deliveryCount;
    private final long timestamp;
    private final Map<String,Object> headers;
    private final long delay;
    
    public static class Builder
    {
        private String uuid = null;
        private byte[] bytes = null;
        private long expiration = -1;
        private int deliveryCount = -1;
        private long timestamp = -1;
        private Map<String,Object> headers = Collections.emptyMap();
        private long delay = -1;
        
        public Builder uuid(String uuid) {this.uuid = uuid; return this;}
        public Builder bytes(byte[] bytes) {this.bytes = bytes; return this;}
        public Builder expiration(long expiration) {this.expiration = expiration; return this;}
        public Builder deliveryCount(int deliveryCount) {this.deliveryCount = deliveryCount; return this;}
        public Builder timestamp(long timestamp) {this.timestamp = timestamp; return this;}
        public Builder headers(Map<String,Object> headers) {this.headers = headers; return this;}
        public Builder delay(long delay) {this.delay = delay; return this;}
        
        public RedisByteMessage build()
        {
            return new RedisByteMessage(this.uuid, this.bytes, this.expiration, this.deliveryCount, this.timestamp, this.headers, this.delay);
        }
    }
    
    public static Builder builder() { return new Builder(); }
    
    protected RedisByteMessage(String uuid, byte[] bytes, long expiration, int deliveryCount, long timestamp, Map<String, Object> headers, long delay)
    {
        this.uuid = uuid;
        this.bytes = bytes;
        this.expiration = expiration;
        this.deliveryCount = deliveryCount;
        this.timestamp = timestamp;
        this.headers = headers;
        this.delay = delay;
    }
    
    @Override
    public String getUUID()
    {
        return uuid;
    }

    @Override
    public byte[] getBytes()
    {
        return bytes;
    }

    @Override
    public long getExpiration()
    {
        return expiration;
    }

    @Override
    public int getDeliveryCount()
    {
        return deliveryCount;
    }

    @Override
    public long getTimestamp()
    {
        return timestamp;
    }

    @Override
    public Map<String, Object> getHeaders()
    {
        return headers;
    }

    @Override
    public long getDelay()
    {
        return delay;
    }
}
