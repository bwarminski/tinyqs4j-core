package co.tinyqs.tinyqs4j.core;

import java.io.IOException;

import co.tinyqs.tinyredis.pool.RedisConfiguration;

public class DefaultInternalContext extends InternalContext
{

    public DefaultInternalContext(RedisConfiguration config)
    {
        super(config);
    }

    @Override
    public boolean canDeserialize(Class<?> clazz)
    {
        return getObjectMapper().canDeserialize(getObjectMapper().getTypeFactory().constructType(clazz));
    }

    @Override
    public <T> T deserialize(byte[] btypes, Class<? extends T> clazz) throws IOException
    {
        return getObjectMapper().readValue(btypes, clazz);
    }

}
