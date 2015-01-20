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
    private RedisTopic topic;
    private RedisConversation conversation;
    
    public ChannelIntegrationTest(InternalContext context, RedisChannel channel, RedisTopic topic, RedisConversation conversation)
    {
        this.channel = channel;
        this.topic = topic;
        this.context = context;
        this.conversation = conversation;
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
    
    public void testTick() throws Exception
    {
        channel.tick();
        ByteMessage message = RedisByteMessage.builder().bytes(SIMPLE_BYTES).expiration(System.currentTimeMillis() + 1000).build();
        channel.send(message);
        channel.tick();
        ByteMessage response = Preconditions.checkNotNull(channel.receive());
        channel.tick();
        channel.release(response);
        Thread.sleep(1000);
        channel.tick();
    }
    
    public void testPublish() throws Exception
    {
        topic.subscribe("integrationTest");
        ByteMessage message = RedisByteMessage.builder().bytes(SIMPLE_BYTES).build();
        int nChannels = topic.publish(message);
        Preconditions.checkState(nChannels == 1, "Expecting one channel");
        ByteMessage response = channel.receive();
        Preconditions.checkState(response != null, "Expecting response on channel");
        topic.unsubscribe("integrationTest");
        nChannels = topic.publish(message);
        Preconditions.checkState(nChannels == 0, "Expecting no channels");
        response = channel.receive();
        Preconditions.checkState(response == null, "Expecting no response");        
    }
    
    public void testConversation() throws Exception
    {
        byte[] pingBytes = context.getObjectMapper().writeValueAsBytes("PING");
        byte[] pongBytes = context.getObjectMapper().writeValueAsBytes("PONG");
        ByteMessage pingMsg = RedisByteMessage.builder().bytes(pingBytes).build();
        ByteMessage pongMsg = RedisByteMessage.builder().bytes(pongBytes).build();
        String claimCheck = conversation.put(pingMsg);
        Preconditions.checkState(claimCheck != null, "Expecting claim check to not be null");
        ByteMessage confPing = conversation.take();
        Preconditions.checkState(confPing != null, "Expecting conf ping to not be null");
        Preconditions.checkState(Arrays.equals(confPing.getBytes(), pingBytes), "Expecing ping types to match");
        conversation.respond(confPing, pongMsg);
        ByteMessage confPong = conversation.wait(claimCheck);
        Preconditions.checkState(confPong != null, "Expecting conf pong to not be null");
        Preconditions.checkState(Arrays.equals(confPong.getBytes(), pongBytes), "Expecting pong types to match");
        conversation.acknowledge(claimCheck, confPong);
    }
    
    public static void main(String[] args) throws Exception
    {
        InternalContext context = new InternalContext(new RedisConfiguration());
        RedisConnection controlChannel = context.getConnection();
        ChannelIntegrationTest test = new ChannelIntegrationTest(context, new RedisChannel(context, "integrationTest"), new RedisTopic(context, "integrationBroadcastTest"), new RedisConversation(context, new RedisChannel(context, "integrationConversationTest"), null));
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
            _flush(controlChannel);
            System.out.println("Testing tick");
            test.testTick();
            _flush(controlChannel);
            System.out.println("Testing topics");
            test.testPublish();
            System.out.println("Testing conversations");
            test.testConversation();
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
