package co.tinyqs.tinyqs4j.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import co.tinyqs.tinyqs4j.api.ByteMessage;
import co.tinyqs.tinyqs4j.api.Channel;
import co.tinyqs.tinyqs4j.api.Datastore;
import co.tinyqs.tinyredis.pool.RedisConfiguration;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class LoadTest
{
    private static final Random random = new Random();
    
    ListeningExecutorService clientService;
    ListeningExecutorService serverService;
    Datastore datastore;
    
    public void warmup() throws Exception
    {
        System.out.println("Warming up...");
        Channel channel = datastore.getChannel("test");
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(TimeUnit.SECONDS) < 20)
        {
            Stopwatch innerWatch = Stopwatch.createStarted();
            List<ListenableFuture<Void>> futures = new ArrayList<>(1000);
            for (int i = 0; i < 500; i++)
            {
                futures.add(clientService.submit(ChannelSend.randomMessage(1024, channel)));
                futures.add(serverService.submit(new ChannelReceive(channel)));
            }
            ListenableFuture<?> done = Futures.allAsList(futures);
            done.get();
//            System.out.println("500 done in " + innerWatch.elapsed(TimeUnit.MILLISECONDS));
        };
        System.out.println("-- Warmup complete --\r\n");
    }
    
    public void run() throws Exception
    {
        _testA();
        _testB();
        _testC();
        _testD();
    }
    
    private void _testA() throws Exception
    {
        System.out.println("-- 20,000 1k messages send, then receive/acknowledge --");
        Channel channel = datastore.getChannel("test");
        
        for (int i = 0; i < 5; i++)
        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<ListenableFuture<Void>> futures = new ArrayList<>(20_000);
            for (int j = 0; j < 20_000; j++)
            {
                futures.add(clientService.submit(ChannelSend.randomMessage(1024, channel)));                
            }
            ListenableFuture<?> done = Futures.allAsList(futures);
            done.get();
            System.out.println("Send step complete in " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
            futures.clear();
            for (int j = 0; j < 20_000; j++)
            {
                futures.add(clientService.submit(new ChannelReceive(channel)));
            }
            done = Futures.allAsList(futures);
            done.get();
            System.out.println("Fully complete in " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        System.out.println("-- Complete --\r\n");
    }
    
    public void _testB() throws Exception
    {
        System.out.println("-- 20,000 1k messages simultaneously --");
        Channel channel = datastore.getChannel("test");
        
        for (int i = 0; i < 5; i++)
        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<ListenableFuture<Void>> futures = new ArrayList<>(40_000);
            for (int j = 0; j < 20_000; j++)
            {
                futures.add(clientService.submit(ChannelSend.randomMessage(1024, channel))); 
                futures.add(clientService.submit(new ChannelReceive(channel)));
            }
            ListenableFuture<?> done = Futures.allAsList(futures);
            done.get();            
            System.out.println("Fully complete in " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        System.out.println("-- Complete --\r\n");
    }
    
    public void _testC() throws Exception
    {
        System.out.println("-- 200,000 32 byte messages simultaneously --");
        Channel channel = datastore.getChannel("test");
        
        for (int i = 0; i < 5; i++)
        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<ListenableFuture<Void>> futures = new ArrayList<>(400_000);
            for (int j = 0; j < 200_000; j++)
            {
                futures.add(clientService.submit(ChannelSend.randomMessage(32, channel))); 
                futures.add(clientService.submit(new ChannelReceive(channel)));
            }
            ListenableFuture<?> done = Futures.allAsList(futures);
            done.get();            
            System.out.println("Fully complete in " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        System.out.println("-- Complete --\r\n");
    }
    
    public void _testD() throws Exception
    {
        System.out.println("-- 200 32k messages simultaneously --");
        Channel channel = datastore.getChannel("test");
        
        for (int i = 0; i < 5; i++)
        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<ListenableFuture<Void>> futures = new ArrayList<>(400);
            for (int j = 0; j < 200; j++)
            {
                futures.add(clientService.submit(ChannelSend.randomMessage(32768, channel))); 
                futures.add(clientService.submit(new ChannelReceive(channel)));
            }
            ListenableFuture<?> done = Futures.allAsList(futures);
            done.get();            
            System.out.println("Fully complete in " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        System.out.println("-- Complete --\r\n");
    }
    
    public static void main(String[] args) throws Exception
    {
        LoadTest test = new LoadTest();
        test = new LoadTest();
        int clientThreads = args.length >= 1 ? Integer.parseInt(args[0]) : 8;
        int serverThreads = args.length >= 2 ? Integer.parseInt(args[1]) : 8;
        test.clientService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(clientThreads));
        test.serverService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(serverThreads));
        RedisConfiguration config = new RedisConfiguration();
        config.setMaxSize(Math.max(clientThreads + serverThreads, 256));
        test.datastore = RedisDatastore.createDefaultDatastore(config);
        
        try
        {
            test.warmup();
            test.run();
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block`
            e.printStackTrace();
        }
        test.serverService.shutdown();
        test.clientService.shutdown();
        System.exit(0);
        
    }
    
    private static class ChannelSend implements Callable<Void>
    {        
        final ByteMessage message;
        final Channel channel;
        
        public static ChannelSend randomMessage(int byteLength, Channel channel)
        {
            byte[] bytes = new byte[byteLength];
            random.nextBytes(bytes);
            return new ChannelSend(RedisByteMessage.builder().bytes(bytes).build(), channel);
        }
        
        ChannelSend(ByteMessage message, Channel channel)
        {
            this.message = message;
            this.channel = channel;
        }
        
        @Override
        public Void call() throws Exception
        {
            channel.send(message);
            return null;
        }        
    }
    
    private static class ChannelReceive implements Callable<Void>
    {
        private final Channel channel;
        
        ChannelReceive(Channel channel)
        {
            this.channel = channel;
        }
        
        @Override
        public Void call() throws Exception
        {
            ByteMessage message = null;
            while (message == null)
                message = channel.receive(60);
            channel.acknowledge(message);
            return null;
        }
        
    }
}
