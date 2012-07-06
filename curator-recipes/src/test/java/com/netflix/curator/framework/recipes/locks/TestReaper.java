package com.netflix.curator.framework.recipes.locks;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestReaper extends BaseClassForTests
{
    @Test
    public void testSparseUseNoReap() throws Exception
    {
        final int   THRESHOLD = 3000;

        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client = makeClient(timing, null);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, THRESHOLD);
            reaper.start();
            reaper.addPath("/one/two/three");

            long        start = System.currentTimeMillis();
            boolean     emptyCountIsCorrect = false;
            while ( ((System.currentTimeMillis() - start) < timing.forWaiting().milliseconds()) && !emptyCountIsCorrect )   // need to loop as the Holder can go in/out of the Reaper's DelayQueue
            {
                emptyCountIsCorrect = (reaper.getEmptyCount("/one/two/three") > 0);
                Thread.sleep(1);
            }
            Assert.assertTrue(emptyCountIsCorrect);

            client.create().forPath("/one/two/three/foo");

            Thread.sleep(2 * (THRESHOLD / Reaper.EMPTY_COUNT_THRESHOLD));
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));
            client.delete().forPath("/one/two/three/foo");

            Thread.sleep(THRESHOLD);
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testReapUntilDelete() throws Exception
    {
        testReapUntilDelete(null);
    }

    @Test
    public void testReapUntilDeleteNamespace() throws Exception
    {
        testReapUntilDelete("test");
    }

     @Test
    public void testReapUntilGone() throws Exception
    {
        testReapUntilGone(null);
    }

    @Test
    public void testReapUntilGoneNamespace() throws Exception
    {
        testReapUntilGone("test");
    }

    @Test
    public void testRemove() throws Exception
    {
        testRemove(null);
    }

    @Test
    public void testRemoveNamespace() throws Exception
    {
        testRemove("test");
    }

    @Test
    public void testSimulationWithLocks() throws Exception
    {
        testSimulationWithLocks(null);
    }

    @Test
    public void testSimulationWithLocksNamespace() throws Exception
    {
        testSimulationWithLocks("test");
    }

    @Test
    public void testWithEphemerals() throws Exception
    {
        testWithEphemerals(null);
    }

    @Test
    public void testWithEphemeralsNamespace() throws Exception
    {
        testWithEphemerals("test");
    }

    @Test
    public void testBasic() throws Exception
    {
        testBasic(null);
    }

    @Test
    public void testBasicNamespace() throws Exception
    {
        testBasic("test");
    }

    private void testReapUntilDelete(String namespace) throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three", Reaper.Mode.REAP_UNTIL_DELETE);
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));

            client.create().forPath("/one/two/three");
            timing.sleepABit();
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    private void testReapUntilGone(String namespace) throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client = makeClient(timing, namespace);
        try
        {
            client.start();
            
            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three", Reaper.Mode.REAP_UNTIL_GONE);
            timing.sleepABit();

            client.create().creatingParentsIfNeeded().forPath("/one/two/three");
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper.addPath("/one/two/three", Reaper.Mode.REAP_UNTIL_GONE);
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }


    private CuratorFramework makeClient(Timing timing, String namespace) throws IOException
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
            .connectionTimeoutMs(timing.connection())
            .sessionTimeoutMs(timing.session())
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1));
        if ( namespace != null )
        {
            builder = builder.namespace(namespace);
        }
        return builder.build();
    }

    private void testRemove(String namespace) throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));

            Assert.assertTrue(reaper.removePath("/one/two/three"));

            client.create().forPath("/one/two/three");
            timing.sleepABit();
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    private void testSimulationWithLocks(String namespace) throws Exception
    {
        final int           LOCK_CLIENTS = 10;
        final int           ITERATIONS = 250;
        final int           MAX_WAIT_MS = 10;

        ExecutorService                     service = Executors.newFixedThreadPool(LOCK_CLIENTS);
        ExecutorCompletionService<Object>   completionService = new ExecutorCompletionService<Object>(service);

        Timing                      timing = new Timing();
        Reaper                      reaper = null;
        final CuratorFramework      client = makeClient(timing, namespace);
        try
        {
            client.start();

            reaper = new Reaper(client, MAX_WAIT_MS / 2);
            reaper.start();
            reaper.addPath("/a/b");

            for ( int i = 0; i < LOCK_CLIENTS; ++i )
            {
                completionService.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            final InterProcessMutex     lock = new InterProcessMutex(client, "/a/b");
                            for ( int i = 0; i < ITERATIONS; ++i )
                            {
                                lock.acquire();
                                try
                                {
                                    Thread.sleep((int)(Math.random() * MAX_WAIT_MS));
                                }
                                finally
                                {
                                    lock.release();
                                }
                            }
                            return null;
                        }
                    }
                );
            }

            for ( int i = 0; i < LOCK_CLIENTS; ++i )
            {
                completionService.take().get();
            }

            Thread.sleep(timing.session());
            timing.sleepABit();

            Stat stat = client.checkExists().forPath("/a/b");
            Assert.assertNull("Child qty: " + ((stat != null) ? stat.getNumChildren() : 0), stat);
        }
        finally
        {
            service.shutdownNow();
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    private void testWithEphemerals(String namespace) throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client2 = null;
        CuratorFramework        client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            client2 = makeClient(timing, namespace);
            client2.start();
            for ( int i = 0; i < 10; ++i )
            {
                client2.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/one/two/three/foo-");
            }

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            client2.close();    // should clear ephemerals
            client2 = null;

            Thread.sleep(timing.session());
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client2);
            Closeables.closeQuietly(client);
        }
    }

    private void testBasic(String namespace) throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }
}
