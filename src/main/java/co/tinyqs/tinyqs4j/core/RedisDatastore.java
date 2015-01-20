package co.tinyqs.tinyqs4j.core;

import com.google.common.base.Preconditions;
import co.tinyqs.tinyqs4j.api.Channel;
import co.tinyqs.tinyqs4j.api.Conversation;
import co.tinyqs.tinyqs4j.api.Datastore;
import co.tinyqs.tinyqs4j.api.Topic;
import co.tinyqs.tinyredis.pool.RedisConfiguration;

public class RedisDatastore implements Datastore
{
    public final InternalContext context;
    
    public static RedisDatastore createDefaultDatastore(RedisConfiguration config)
    {
        return new RedisDatastore(new InternalContext(config));
    }
    
    protected RedisDatastore(InternalContext context)
    {
        this.context = Preconditions.checkNotNull(context);
    }
    
    public Channel getChannel(String name)
    {
        return context.getChannel(name);
    }
    
    public Topic getTopic(String name)
    {
        return new RedisTopic(context, name);
    }
    
    public <T> RedisTypedMessageBuilder<T> buildMessage()
    {
        return new RedisTypedMessageBuilder<T>(context);
    }
    
    public void close()
    {
        context.close();
    }

    @Override
    public Conversation getConversation(String name) {
        return new RedisConversation(context, context.getChannel(name), null);
    }
}
