package com.netflix.curator;

import com.google.common.io.Closeables;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.session.SessionFailedException;
import com.netflix.curator.session.SessionFailedHandler;
import com.netflix.curator.test.KillSession;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestSessionSafeRetry extends BaseClassForTests
{
    @Test
    public void testMultipleThreads() throws Exception
    {
        final int           ITERATIONS = 10;

        final Timing                    timing = new Timing();
        final CuratorZookeeperClient    client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, new ExponentialBackoffRetry(timing.milliseconds(), 3));
        try
        {
            client.start();

            ExecutorCompletionService<Void>     completionService = new ExecutorCompletionService<Void>(Executors.newCachedThreadPool());

            completionService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        for ( int i = 0; i < ITERATIONS; ++i )
                        {
                            noRetryFailure(client);
                            timing.sleepABit();
                        }
                        return null;
                    }
                }
            );

            completionService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        for ( int i = 0; i < ITERATIONS; ++i )
                        {
                            RetryLoop.callWithRetry
                            (
                                client,
                                new Callable<Object>()
                                {
                                    @Override
                                    public Object call() throws Exception
                                    {
                                        Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));  // should continue succeeding regardless of session failure
                                        return null;
                                    }
                                }
                            );
                        }
                        return null;
                    }
                }
            );

            completionService.take().get();
            completionService.take().get();
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testNoRetryFailure() throws Exception
    {
        Timing                          timing = new Timing();
        final CuratorZookeeperClient    client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, new RetryOneTime(1));
        try
        {
            client.start();

            noRetryFailure(client);
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testBasicFailure() throws Exception
    {
        Timing                          timing = new Timing();
        final CuratorZookeeperClient    client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, new RetryOneTime(1));
        try
        {
            client.start();

            final AtomicBoolean         firstTime = new AtomicBoolean(true);

            final AtomicBoolean         didFail = new AtomicBoolean(false);
            SessionFailedHandler        sessionFailedHandler = new SessionFailedHandler()
            {
                @Override
                public boolean shouldAttemptToRetry()
                {
                    didFail.set(true);
                    return true;
                }
            };

            RetryLoop.sessionSafeRetry
            (
                client,
                sessionFailedHandler,
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));

                        if ( firstTime.compareAndSet(true, false) )
                        {
                            KillSession.kill(client.getZooKeeper(), server.getConnectString());
                        }

                        client.getZooKeeper();
                        client.blockUntilConnectedOrTimedOut();
                        client.getZooKeeper().exists("/foo/bar", false);
                        return null;
                    }
                }
            );

            Assert.assertTrue(didFail.get());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    private void noRetryFailure(final CuratorZookeeperClient client) throws Exception
    {
        try
        {
            SessionFailedHandler sessionFailedHandler = new SessionFailedHandler()
            {
                @Override
                public boolean shouldAttemptToRetry()
                {
                    return false;
                }
            };
            RetryLoop.sessionSafeRetry
            (
                client,
                sessionFailedHandler,
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                        KillSession.kill(client.getZooKeeper(), server.getConnectString());

                        client.getZooKeeper();
                        client.blockUntilConnectedOrTimedOut();
                        client.getZooKeeper().exists("/foo/bar", false);
                        return null;
                    }
                }
            );
            Assert.fail();
        }
        catch ( SessionFailedException dummy )
        {
            // correct
        }
    }
}
