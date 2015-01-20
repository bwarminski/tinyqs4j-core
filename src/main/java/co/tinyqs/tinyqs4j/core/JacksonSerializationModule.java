package co.tinyqs.tinyqs4j.core;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonSerializationModule implements SerializationModule
{
    private final ObjectMapper objectMapper;
    
    public JacksonSerializationModule(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    /* (non-Javadoc)
     * @see co.tinyqs.tinyqs4j.core.SerializationModule#canDeserialize(java.lang.Class)
     */
    @Override
    public boolean canDeserialize(Class<?> clazz)
    {
        return objectMapper.canDeserialize(objectMapper.getTypeFactory().constructType(clazz));
    }

    /* (non-Javadoc)
     * @see co.tinyqs.tinyqs4j.core.SerializationModule#deserialize(byte[], java.lang.Class)
     */
    @Override
    public <T> T deserialize(byte[] btypes, Class<? extends T> clazz) throws IOException
    {
        return objectMapper.readValue(btypes, clazz);
    }

    @Override
    public boolean canSerialize(Object obj)
    {
        return obj == null || objectMapper.canSerialize(obj.getClass());
    }

    @Override
    public byte[] serialize(Object obj) throws IOException
    {
        return objectMapper.writeValueAsBytes(obj);
    }

}
