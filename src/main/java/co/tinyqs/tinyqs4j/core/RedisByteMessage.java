package co.tinyqs.tinyqs4j.core;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.MessageBuilder;

import java.util.Map;

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
    
    public static class Builder extends MessageBuilder<RedisByteMessage>
    {    
        public static Builder wrap(ByteMessage message)
        {
            Builder builder = new Builder();
            builder.uuid = message.getUUID();
            builder.bytes = message.getBytes();
            builder.expiration = message.getExpiration();
            builder.deliveryCount = message.getDeliveryCount();
            builder.timestamp = message.getTimestamp();
            builder.headers = message.getHeaders();
            builder.delay = message.getDelay();
            return builder;
        }
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
