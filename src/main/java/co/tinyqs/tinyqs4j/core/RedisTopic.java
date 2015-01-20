package co.tinyqs.tinyqs4j.core;

import java.io.IOException;

import com.google.common.base.Preconditions;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.Topic;
import co.tinyqs.tinyredis.RedisConnection;
import co.tinyqs.tinyredis.RedisReply;

public class RedisTopic implements Topic
{
    private final InternalContext context;
    private final String name;
    private final String internalName;
    
    protected RedisTopic(InternalContext context, String name)
    {
        this.context = context;
        this.name = name;
        this.internalName = "t:"+name;
    }
    
    public String getName()
    {
        return name;
    }
    
    @Override
    public int publish(ByteMessage message) throws IOException
    {
        int nChannels = 0;
        try
        {
            RedisConnection conn = context.getConnection();
            try
            {
                RedisReply reply = conn.sendCommand("SMEMBERS %s:subscribers", internalName);
                if (reply.isError())
                {
                    throw new IOException("Error getting set members " + reply.getString());
                }
                Preconditions.checkState(reply.isArray(), "Expecting array reply from SMEMBERS");
                if (reply.getElements().length > 0)
                {
                    conn.sendCommand("MULTI");
                    for (RedisReply element : reply.getElements())
                    {
                        Preconditions.checkState(element.isString(), "Expecting string element reply from SMEMBERS");
                        RedisChannel channel = context.getChannel(element.getString());
                        channel.sendMessage(message, conn, true);
                        nChannels++;
                    }
                    conn.appendCommand("EXEC");
                    for (int i = 0; i < nChannels; i++)
                    {
                        RedisReply queued = conn.getReply();
                        Preconditions.checkState(queued.isStatus(), "Expecting simple status reply from command sent");                        
                    }
                    RedisReply execReply = conn.getReply();
                    if (execReply.isError())
                    {
                        throw new IOException("Error executing EXEC on channel send " + execReply.getString());
                    }
                    Preconditions.checkState(execReply.isArray(), "Expecting array reply from EXEC");
                    for (RedisReply element : execReply.getElements())
                    {
                        if (element.isError())
                        {
                            throw new IOException("Error sending to channel " + element.getString());
                        }
                    }                    
                }
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
        return nChannels;
    }

    @Override
    public void subscribe(String destination) throws IOException
    {
        try
        {
            RedisConnection conn = context.getConnection();
            try
            {
                RedisReply reply = conn.sendCommand("SADD %s:subscribers %s", internalName, destination);
                if (reply.isError())
                {
                    throw new IOException("SADD Error: " + reply.getString());
                }
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
    }

    @Override
    public void unsubscribe(String destination) throws IOException
    {
        try
        {
            RedisConnection conn = context.getConnection();
            try
            {
                RedisReply reply = conn.sendCommand("SREM %s:subscribers %s", internalName, destination);
                if (reply.isError())
                {
                    throw new IOException("SREM Error: " + reply.getString());
                }
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
    }

}
