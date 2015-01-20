package co.tinyqs.tinyqs4j.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.Conversation;
import co.tinyqs.tinyqs4j.api.Message;
import co.tinyqs.tinyredis.RedisConnection;
import co.tinyqs.tinyredis.RedisReply;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

public class RedisConversation implements Conversation 
{
    public static final int DEFAULT_LOCK_TIME = 30000;
    public static final String H_CLAIM_CHECK = "tinyqs-ReplyTo";
    private static final int MAX_LOCK_TRIES = 3;
    private static final Supplier<String> DEFAULT_UUID_SUPPLIER = new Supplier<String>(){

        @Override
        public String get() {
            return UUID.randomUUID().toString();
        }
    };
    
    private final InternalContext context;
    private final RedisChannel wrappedChannel;
    private final Supplier<String> uuidSupplier;        
    
    protected RedisConversation(InternalContext context, RedisChannel channel, Supplier<String> uuidSupplier)
    {
        this.context = context;
        this.wrappedChannel = channel;
        this.uuidSupplier = Objects.firstNonNull(uuidSupplier, DEFAULT_UUID_SUPPLIER);
    }
    
    @Override
    public String getName() 
    {
        return wrappedChannel.getName();
    }

    @Override
    public String put(ByteMessage message) throws IOException 
    {
        String uuid = null;
        try
        {
            RedisConnection conn = context.getConnection();           
            try
            {
                // Lock UUID
                
                boolean locked = false;
                
                for (int i = 0; i < MAX_LOCK_TRIES && !locked; i++)
                {
                    uuid = uuidSupplier.get();
                    long expiration = message.getExpiration() > 0 ? message.getExpiration() - System.currentTimeMillis() : DEFAULT_LOCK_TIME;
                    RedisReply reply = conn.sendCommand("SET lock:%s 0 PX %s NX", uuid, expiration);
                    if (reply.isError())
                    {
                        throw new IOException(reply.getString());
                    }
                    locked = !reply.isNil();
                }
                Preconditions.checkState(locked, "Unable to lock reply channel");   
                
                // Send message on channel with replyTo header set
                Map<String,Object> headers = new HashMap<>(message.getHeaders());
                headers.put(H_CLAIM_CHECK, uuid);
                message = RedisByteMessage.Builder.wrap(message).headers(headers).build();
                wrappedChannel.send(message);
                
                context.releaseConnection(conn);
                conn = null;
            }
            finally
            {
                if (conn != null)
                {
                    context.destroyConnection(conn);
                }
            }
            
            
        }
        catch (IOException e)
        {
            throw e;            
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
        return uuid;
    }

    @Override
    public ByteMessage take() throws IOException 
    {
        return wrappedChannel.receive();
    }

    @Override
    public ByteMessage take(int timeoutSec) throws IOException 
    {
        return wrappedChannel.receive(timeoutSec);
    }

    @Override
    public void respond(ByteMessage request, ByteMessage response) throws IOException 
    {
        String claimCheck = (String) Preconditions.checkNotNull(request.getHeaders().get(H_CLAIM_CHECK), "Malformed request - missing reply header");
        RedisChannel responseChannel = context.getChannel(claimCheck);
        responseChannel.send(response);
        wrappedChannel.acknowledge(request);
    }

    @Override
    public ByteMessage wait(String claimCheck) throws IOException 
    {
        RedisChannel responseChannel = context.getChannel(claimCheck);
        return responseChannel.receive();
    }

    @Override
    public ByteMessage wait(String claimCheck, int timeoutSec) throws IOException 
    {
        RedisChannel responseChannel = context.getChannel(claimCheck);
        return responseChannel.receive(timeoutSec);
    }

    @Override
    public <T> Message<T> take(Class<? extends T> msgClass) throws IOException {
        return wrappedChannel.receive(msgClass);
    }

    @Override
    public <T> Message<T> take(Class<? extends T> msgClass, int timeoutSec) throws IOException {
        return wrappedChannel.receive(msgClass, timeoutSec);
    }

    @Override
    public <T> Message<T> wait(Class<? extends T> msgClass, String claimCheck) throws IOException {
        RedisChannel responseChannel = context.getChannel(claimCheck);
        return responseChannel.receive(msgClass);
    }

    @Override
    public <T> Message<T> wait(Class<? extends T> msgClass, String claimCheck, int timeoutSec)
        throws IOException {
        RedisChannel responseChannel = context.getChannel(claimCheck);
        return responseChannel.receive(msgClass, timeoutSec);
    }

    @Override
    public void acknowledge(String claimCheck, ByteMessage response) throws IOException {
        RedisChannel responseChannel = context.getChannel(claimCheck);
        responseChannel.acknowledge(response);
    }

}
