package co.tinyqs.tinyqs4j.core;

import java.io.IOException;

public interface SerializationModule
{
    public boolean canDeserialize(Class<?> clazz);

    public <T> T deserialize(byte[] btypes, Class<? extends T> clazz) throws IOException;

    public boolean canSerialize(Object obj);
    
    public byte[] serialize(Object obj) throws IOException;
}