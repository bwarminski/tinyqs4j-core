package co.tinyqs.tinyqs4j.core;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.base.Preconditions;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.Message;
import co.tinyqs.tinyredis.RedisConnection;
import co.tinyqs.tinyredis.pool.RedisConfiguration;

public class ChannelIntegrationTest
{
    private static final byte[] SIMPLE_BYTES = new byte[] {(byte) 0, (byte) 1};
    private static final String HELLO_WORLD = "Hello World";
    
    private InternalContext context;
    private RedisChannel channel;
    
    public ChannelIntegrationTest(InternalContext context, RedisChannel channel)
    {
        this.channel = channel;
        this.context = context;
    }
    
    public void testSimpleSend() throws Exception
    {
        ByteMessage message = RedisByteMessage.builder().bytes(SIMPLE_BYTES).build();
        channel.send(message);
    }
    
    public void testReceive() throws Exception
    {
        testSimpleSend();
        ByteMessage response = channel.receive();
        Preconditions.checkState(response != null, "Response is null");
        Preconditions.checkState(Arrays.equals(response.getBytes(), SIMPLE_BYTES), "Response arrays do not match");
        Preconditions.checkState(response.getUUID() != null, "Returned uuid is null");
        ByteMessage nullResponse = channel.receive();
        Preconditions.checkState(nullResponse == null, "Response should be null");
    }
    
    public void testDelayedSendReceive() throws Exception
    {
        ByteMessage message = RedisByteMessage.builder().bytes(SIMPLE_BYTES).delay(System.currentTimeMillis() + 100000).build();
        channel.send(message);
        ByteMessage nullResponse = channel.receive();
        Preconditions.checkState(nullResponse == null, "Response should be null");
    }
    
    public void testPastDelayedSendReceive() throws Exception
    {
        ByteMessage message = RedisByteMessage.builder().bytes(SIMPLE_BYTES).delay(System.currentTimeMillis() - 1).build();
        channel.send(message);
        ByteMessage nullResponse = channel.receive();
        Preconditions.checkState(nullResponse != null, "Response should not be null");
    }
    
    public void testAlreadyExpiredSendReceive() throws Exception
    {
        ByteMessage message = RedisByteMessage.builder().bytes(SIMPLE_BYTES).expiration(System.currentTimeMillis() - 1).build();
        channel.send(message);
        ByteMessage nullResponse = channel.receive();
        Preconditions.checkState(nullResponse == null, "Response should be null");
    }
    
    public void testSendReceiveTyped() throws Exception
    {
        byte[] helloWorldBytes = context.getObjectMapper().writeValueAsBytes(HELLO_WORLD);
        ByteMessage byteMessage = RedisByteMessage.builder().bytes(helloWorldBytes).build();
        Message<String> message = Message.wrap(byteMessage, HELLO_WORLD);
        channel.send(message);
        Message<String> response = channel.receive(String.class);
        Preconditions.checkState(response != null && HELLO_WORLD.equals(response.getPayload()), "Response does not equal message sent");
    }
    
    public void testSendReceiveRelease() throws Exception
    {
        testSimpleSend();
        ByteMessage response = channel.receive();
        channel.release(response);
        response = channel.receive();
        Preconditions.checkState(response != null, "Response is null");
        Preconditions.checkState(Arrays.equals(response.getBytes(), SIMPLE_BYTES), "Response arrays do not match");
    }
    
    public void testSendReceiveAck() throws Exception
    {
        testSimpleSend();
        ByteMessage response = channel.receive();
        channel.acknowledge(response);
        response = channel.receive();
        Preconditions.checkState(response == null, "Response should be null");
    }
    
    public static void main(String[] args) throws Exception
    {
        DefaultInternalContext context = new DefaultInternalContext(new RedisConfiguration());
        RedisConnection controlChannel = context.getConnection();
        ChannelIntegrationTest test = new ChannelIntegrationTest(context, new RedisChannel(context, "integrationTest"));
        try
        {
            controlChannel.exceptionOnError(true);
            _flush(controlChannel);
            System.out.println("Testing send()");
            test.testSimpleSend();
            _flush(controlChannel);
            System.out.println("Testing receive()");
            test.testReceive();
            _flush(controlChannel);
            test.testDelayedSendReceive();
            _flush(controlChannel);
            test.testPastDelayedSendReceive();
            _flush(controlChannel);
            test.testAlreadyExpiredSendReceive();
            System.out.println("Testing send and receive of a typed message");
            _flush(controlChannel);
            test.testSendReceiveTyped();
            System.out.println("Testing release");
            _flush(controlChannel);
            test.testSendReceiveRelease();
            System.out.println("Testing acknowledge");
            _flush(controlChannel);
            test.testSendReceiveAck();
            System.out.println("Testing complete");
        }
        finally
        {
            context.close();
        }
    }

    private static void _flush(RedisConnection controlChannel) throws IOException
    {
        controlChannel.sendCommand("FLUSHDB");
    }
}
