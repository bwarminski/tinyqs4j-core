package co.tinyqs.tinyqs4j.core;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.Message;
import co.tinyqs.tinyqs4j.api.TypedMessageBuilder;
import com.google.common.base.Preconditions;

import java.io.IOException;

public class RedisTypedMessageBuilder<T> extends TypedMessageBuilder<T>
{
    private final InternalContext context;
    
    public RedisTypedMessageBuilder(InternalContext context)
    {
        this.context = context;
    }
    
    @Override
    public Message<T> build()
    {
        Preconditions.checkState(context.getSerializer().canSerialize(payload), "Must be able to serialize payload");
        try
        {
            this.bytes = context.getSerializer().serialize(payload);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        ByteMessage byteMessage = new RedisByteMessage(uuid, bytes, expiration, deliveryCount, timestamp, headers, delay);
        return Message.wrap(byteMessage, payload);
    }

}
