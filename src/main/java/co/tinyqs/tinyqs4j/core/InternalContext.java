package co.tinyqs.tinyqs4j.core;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import co.tinyqs.tinyredis.RedisConnection;
import co.tinyqs.tinyredis.RedisSerializer;
import co.tinyqs.tinyredis.pool.RedisConfiguration;
import co.tinyqs.tinyredis.pool.RedisConnectionPool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public abstract class InternalContext
{
    private final RedisConnectionPool connectionPool;
    private final ScheduledExecutorService service;
    private final ObjectMapper mapper;
    private final ConcurrentHashMap<String, Object> channels = new ConcurrentHashMap<String,Object>();
    
    public InternalContext(RedisConfiguration config)
    {        
        this.service = Preconditions.checkNotNull(Executors.newSingleThreadScheduledExecutor());
        this.mapper = new ObjectMapper();                
        config.setSerializers(ImmutableList.<RedisSerializer>of(new InternalRedisSerializer(mapper)));
        this.connectionPool = Preconditions.checkNotNull(new RedisConnectionPool(config, Scripts.ALL));      
    }
    
    public InternalContext(RedisConnectionPool connectionPool, ScheduledExecutorService service, ObjectMapper mapper)
    {
        this.connectionPool = Preconditions.checkNotNull(connectionPool);
        this.service = Preconditions.checkNotNull(service);
        this.mapper = mapper;
        
        // Schedule ticks
    }
    
    public RedisConnection getConnection() throws Exception
    {
        return connectionPool.borrowObject();
    }
    
    public void releaseConnection(RedisConnection conn) throws Exception
    {
        connectionPool.returnObject(conn);
    }
    
    public void destroyConnection(RedisConnection conn) throws Exception
    {
        connectionPool.invalidateObject(conn);
    }
    
    public ObjectMapper getObjectMapper()
    {
        return mapper;
    }
    
    public void close()
    {
        this.service.shutdown();
        this.connectionPool.close();
    }
    
    public void registerChannel(String channel)
    {
        channels.put(channel, null);
    }
    
    public abstract boolean canDeserialize(Class<?> clazz);
    public abstract <T> T deserialize(byte[] btypes, Class<? extends T> clazz) throws IOException;
}
