package co.tinyqs.tinyqs4j.core;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.Message;
import co.tinyqs.tinyredis.RedisSerializer;

public class InternalRedisSerializer implements RedisSerializer
{
    private final ObjectMapper mapper;
    
    InternalRedisSerializer(ObjectMapper mapper)
    {
        this.mapper = mapper;
    }
    
    @Override
    public boolean canSerialize(Object obj)
    {
        return obj == null || obj instanceof ByteMessage || mapper.canSerialize(obj.getClass());
    }

    @Override
    public byte[] serialize(Object obj) throws IOException
    {
        if (obj instanceof ByteMessage)
        {
            return ((ByteMessage) obj).getBytes();
        }
        else
        {
            return mapper.writeValueAsBytes(obj);
        }                
    }

}
