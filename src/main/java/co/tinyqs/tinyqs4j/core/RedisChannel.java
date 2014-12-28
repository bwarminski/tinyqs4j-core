package co.tinyqs.tinyqs4j.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.Channel;
import co.tinyqs.tinyqs4j.api.Message;
import co.tinyqs.tinyredis.RedisConnection;
import co.tinyqs.tinyredis.RedisReply;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class RedisChannel implements Channel
{
    private final InternalContext context;
    private final String name;
    
    protected RedisChannel(InternalContext context, String name)
    {
        this.context = context;
        this.name = "c:"+name;
    }
    
    @Override
    public void send(ByteMessage message) throws IOException
    {
        try
        {
            RedisConnection conn = context.getConnection();
            try
            {   
                String format = "EVALSHA " + Scripts.SEND.getSHA() + " 8 " + name + ":counter " + name + ":active " + name + ":pending data " + name + 
                        ":expirations deliveries timestamps headers %s %s %b %s %s %b";
                RedisReply reply = conn.sendCommand(format, name, System.currentTimeMillis(), message.getHeaders(), message.getDelay(), 
                                                    message.getExpiration(), message);
                if (reply.isError())
                {
                    throw new IOException("Unexpected error from send: " + reply.getString());
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

    private ByteMessage _receive(int timeoutSec) throws IOException
    {
        ByteMessage result = null;
        try
        {
            RedisConnection conn = context.getConnection();
            try
            {
                RedisReply reply;
                if (timeoutSec > 0)
                {
                    reply = conn.sendCommand("BRPOPLPUSH %s:active %s:reserved %s", name, name, timeoutSec);
                }
                else
                {
                    reply = conn.sendCommand("RPOPLPUSH %s:active %s:reserved", name, name);
                }
                if (reply.isString())
                {
                    String uuid = reply.getString();
                    String format = "EVALSHA " + Scripts.RECEIVE.getSHA() + " 7 " + name + ":reserved " + name + ":pending data " + name + 
                            ":expirations deliveries timestamps headers %s %s";
                            
                    RedisReply dataReply = conn.sendCommand(format, uuid, System.currentTimeMillis());
                    Preconditions.checkState(dataReply.isArray(), "Expecting array result from RECEIVE script");
                    RedisReply[] elements = dataReply.getElements();
                    Preconditions.checkState(elements.length % 2 == 0, "Expecting even number of elements from data reply");
                    RedisByteMessage.Builder builder = RedisByteMessage.builder().uuid(uuid);
                    for (int i = 0; i < elements.length; i = i + 2)
                    {
                        RedisReply element = elements[i];
                        Preconditions.checkState(element.isString(), "Expecting header reply to be a string");
                        ObjectMapper objectMapper = context.getObjectMapper();
                        switch (element.getString())
                        {
                        case "headers":
                        {
                            RedisReply headerReply = elements[i+1];
                            Preconditions.checkState(headerReply.isString(), "Expected header section to be string");
                            Map<String,Object> headers = new HashMap<String,Object>();
                            
                            Iterator<Entry<String, JsonNode>> iter = objectMapper.readTree(headerReply.getBytes()).fields();
                            while (iter.hasNext())
                            {
                                Entry<String,JsonNode> entry = iter.next();
                                if (entry.getValue().isNumber())
                                {
                                    headers.put(entry.getKey(), entry.getValue().decimalValue());
                                }
                                else if (entry.getValue().isNull())
                                {
                                    headers.put(entry.getKey(), null);
                                }
                                else if (entry.getValue().isBoolean())
                                {
                                    headers.put(entry.getKey(), entry.getValue().booleanValue());
                                }
                                else if (entry.getValue().isTextual())
                                {
                                    headers.put(entry.getKey(), entry.getValue().textValue());
                                }
                                else
                                {
                                    throw new JsonMappingException("Unable to construct valid value from entry " + entry.getValue().getNodeType());
                                }
                            }
                            builder.headers(headers);
                            break;
                        }
                        case "data":
                        {
                            RedisReply bytesReply = elements[i+1];
                            Preconditions.checkState(bytesReply.isString(), "Expecting bulk string reply for data");
                            builder.bytes(bytesReply.getBytes());
                            break;
                        }
                        case "expiration":
                        {
                            RedisReply expirationReply = elements[i+1];
                            if (!expirationReply.isNil())
                            {
                                Preconditions.checkState(expirationReply.isString(), "Expected string representation of double for expiration");
                                builder.expiration(objectMapper.readTree(expirationReply.getBytes()).asLong(-1));
                            }
                            break;
                        }
                        case "deliveries":
                        {
                            RedisReply deliveriesReply = elements[i+1];
                            Preconditions.checkState(deliveriesReply.isString(), "Expected string representation of integer for delivery count");
                            builder.deliveryCount(objectMapper.readTree(deliveriesReply.getBytes()).asInt(-1));
                            break;
                        }
                        case "timestamp":
                            RedisReply timestampReply = elements[i+1];
                            Preconditions.checkState(timestampReply.isString(), "Expected string representation of long for timestamp");
                            builder.timestamp(objectMapper.readTree(timestampReply.getBytes()).asLong(-1));
                            break;
                        }                        
                    }
                    result = builder.build();
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
        return result;
    }
    
    @Override
    public ByteMessage receive() throws IOException
    {
        return _receive(-1);
    }

    @Override
    public ByteMessage receive(int timeoutSec) throws IOException, TimeoutException
    {
        return _receive(timeoutSec);
    }

    @Override
    public <T> Message<T> receive(Class<? extends T> msgClass) throws IOException
    {
        Preconditions.checkArgument(context.canDeserialize(msgClass), "Must be able to deserialize the message class");
        ByteMessage byteMessage = _receive(-1);
        return Message.wrap(byteMessage, context.deserialize(byteMessage.getBytes(), msgClass));
    }

    @Override
    public <T> Message<T> receive(Class<? extends T> msgClass, int timeoutSec) throws IOException, TimeoutException
    {
        Preconditions.checkArgument(context.canDeserialize(msgClass), "Must be able to deserialize the message class");
        ByteMessage byteMessage = _receive(timeoutSec);
        return Message.wrap(byteMessage, context.deserialize(byteMessage.getBytes(), msgClass));
    }

    @Override
    public void release(ByteMessage message) throws IOException
    {
        Preconditions.checkNotNull(message, "Message may not be null");
        try
        {
            RedisConnection conn = context.getConnection();
            try
            {
                RedisReply reply = conn.sendCommand("EVALSHA %s 3 %s:reserved %s:pending %s:active %s", Scripts.RELEASE.getSHA(), name, name, name, message.getUUID());
                if (reply.isError())
                {
                    throw new IOException(reply.getString());
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
    public void acknowledge(ByteMessage message) throws IOException
    {
        Preconditions.checkNotNull(message, "Message may not be null");
        try
        {
            RedisConnection conn = context.getConnection();
            try
            {
                RedisReply reply = conn.sendCommand("EVALSHA " + Scripts.ACKNOWLEDGE.getSHA() + " 8 " + name + ":reserved " + name + ":active " + name + ":pending data " +
                        name + ":expirations deliveries timestamps headers " + message.getUUID());
                if (reply.isError())
                {
                    throw new IOException(reply.getString());
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
