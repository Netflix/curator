package com.netflix.curator.framework.recipes.queue;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestBoundedDistributedQueue extends BaseClassForTests
{
    private static final QueueSerializer<String> serializer = new QueueSerializer<String>()
    {
        @Override
        public byte[] serialize(String item)
        {
            return item.getBytes();
        }

        @Override
        public String deserialize(byte[] bytes)
        {
            return new String(bytes);
        }
    };

    @Test
    public void         testMulti() throws Exception
    {
        final String        PATH = "/queue";
        final int           CLIENT_QTY = 4;
        final int           MAX_ITEMS = 10;
        final int           ADD_ITEMS = MAX_ITEMS * 100;
        final int           SLOP_FACTOR = 2;

        final QueueConsumer<String>     consumer = new QueueConsumer<String>()
        {
            @Override
            public void consumeMessage(String message) throws Exception
            {
                Thread.sleep(10);
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
            }
        };

        final Timing                        timing = new Timing();
        final ExecutorService               executor = Executors.newCachedThreadPool();
        ExecutorCompletionService<Void>     completionService = new ExecutorCompletionService<Void>(executor);

        final CuratorFramework              client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath(PATH);

            final List<Integer> counts = new CopyOnWriteArrayList<Integer>();
            final Object        lock = new Object();
            completionService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        Watcher     watcher = new Watcher()
                        {
                            @Override
                            public void process(WatchedEvent event)
                            {
                                synchronized(lock)
                                {
                                    lock.notifyAll();
                                }
                            }
                        };

                        while ( !Thread.currentThread().isInterrupted() && client.isStarted() )
                        {
                            synchronized(lock)
                            {
                                int     size = client.getChildren().usingWatcher(watcher).forPath(PATH).size();
                                counts.add(size);
                                lock.wait();
                            }
                        }
                        return null;
                    }
                }
            );

            for ( int i = 0; i < CLIENT_QTY; ++i )
            {
                final int       index = i;
                completionService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            CuratorFramework            client = null;
                            DistributedQueue<String>    queue = null;

                            try
                            {
                                client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
                                client.start();
                                queue = QueueBuilder.builder(client, consumer, serializer, PATH).executor(executor).maxItems(MAX_ITEMS).putInBackground(false).buildQueue();
                                queue.start();

                                for ( int i = 0; i < ADD_ITEMS; ++i )
                                {
                                    queue.put("" + index + "-" + i);
                                }
                            }
                            finally
                            {
                                Closeables.closeQuietly(queue);
                                Closeables.closeQuietly(client);
                            }
                            return null;
                        }
                    }
                );
            }

            for ( int i = 0; i < CLIENT_QTY; ++i )
            {
                completionService.take().get();
            }

            for ( int count : counts )
            {
                Assert.assertTrue(counts.toString(), count <= (MAX_ITEMS * SLOP_FACTOR));
            }
        }
        finally
        {
            executor.shutdownNow();
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void         testSimple() throws Exception
    {
        Timing                      timing = new Timing();
        DistributedQueue<String>    queue = null;
        CuratorFramework            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final List<String>          messages = new CopyOnWriteArrayList<String>();
            final CountDownLatch        latch = new CountDownLatch(1);
            final Semaphore             semaphore = new Semaphore(0);
            QueueConsumer<String>       consumer = new QueueConsumer<String>()
            {
                @Override
                public void consumeMessage(String message) throws Exception
                {
                    semaphore.acquire();
                    messages.add(message);
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            queue = QueueBuilder.builder(client, consumer, serializer, "/queue").executor(Executors.newSingleThreadExecutor()).maxItems(1).buildQueue();
            queue.start();

            QueuePutListener<String>    listener = new QueuePutListener<String>()
            {
                @Override
                public void putCompleted(String item)
                {
                    latch.countDown();
                }

                @Override
                public void putMultiCompleted(MultiItem<String> items)
                {
                }
            };
            queue.getPutListenerContainer().addListener(listener);

            Assert.assertTrue(queue.put("1", timing.milliseconds(), TimeUnit.MILLISECONDS));   // should end up in consumer
            Assert.assertTrue(queue.put("2", timing.milliseconds(), TimeUnit.MILLISECONDS));   // should sit blocking in DistributedQueue
            Assert.assertTrue(timing.awaitLatch(latch));
            Assert.assertFalse(queue.put("3", timing.multiple(.5).milliseconds(), TimeUnit.MILLISECONDS));

            semaphore.release(100);
            Assert.assertTrue(queue.put("3", timing.milliseconds(), TimeUnit.MILLISECONDS));
            Assert.assertTrue(queue.put("4", timing.milliseconds(), TimeUnit.MILLISECONDS));
            Assert.assertTrue(queue.put("5", timing.milliseconds(), TimeUnit.MILLISECONDS));

            for ( int i = 0; i < 5; ++i )
            {
                if ( messages.size() == 3 )
                {
                    break;
                }
                timing.sleepABit();
            }

            Assert.assertEquals(messages, Arrays.asList("1", "2", "3", "4", "5"));
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }
}
