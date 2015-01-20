package co.tinyqs.tinyqs4j.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import co.tinyqs.tinyredis.RedisConnection;
import co.tinyqs.tinyredis.RedisSerializer;
import co.tinyqs.tinyredis.pool.RedisConfiguration;
import co.tinyqs.tinyredis.pool.RedisConnectionPool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class InternalContext
{
    private final RedisConnectionPool connectionPool;
    private final ScheduledExecutorService service;
    private final ObjectMapper mapper;
    private final ConcurrentHashMap<String, RedisChannel> channels = new ConcurrentHashMap<String,RedisChannel>();
    private final SerializationModule serialization;
    
    public InternalContext(RedisConfiguration config)
    {        
        this.service = Preconditions.checkNotNull(Executors.newSingleThreadScheduledExecutor());
        this.mapper = new ObjectMapper();                
        config.setSerializers(ImmutableList.<RedisSerializer>of(new InternalRedisSerializer(mapper)));
        this.connectionPool = Preconditions.checkNotNull(new RedisConnectionPool(config, Scripts.ALL));   
        this.serialization = new JacksonSerializationModule(this.mapper);
        _scheduleTicks();
    }
    
    public InternalContext(RedisConnectionPool connectionPool, ScheduledExecutorService service, ObjectMapper mapper, SerializationModule serialization)
    {
        this.connectionPool = Preconditions.checkNotNull(connectionPool);
        this.service = Preconditions.checkNotNull(service);
        this.mapper = Preconditions.checkNotNull(mapper);
        this.serialization = Preconditions.checkNotNull(serialization);
        _scheduleTicks();
    }
    
    private void _scheduleTicks()
    {
        service.scheduleWithFixedDelay(new Runnable(){

            @Override
            public void run()
            {
                for (RedisChannel channel : channels.values())
                {
                    channel.tick();
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
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
    
    public RedisChannel getChannel(String name)
    {
        RedisChannel channel = new RedisChannel(this, name);
        return Objects.firstNonNull(channels.putIfAbsent(channel.getInternalName(), channel), channel);
    }
    
    public Set<String> getRegisteredChannels()
    {
        return channels.keySet();
    }
    
    public SerializationModule getSerializer()
    {
        return serialization;
    }
}
