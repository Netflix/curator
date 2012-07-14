/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.queue;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TestDistributedDelayQueue extends BaseClassForTests
{
    @Test
    public void     testLateAddition() throws Exception
    {
        Timing                          timing = new Timing();
        DistributedDelayQueue<Long>     queue = null;
        CuratorFramework                client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            BlockingQueueConsumer<Long> consumer = new BlockingQueueConsumer<Long>(Mockito.mock(ConnectionStateListener.class));
            queue = QueueBuilder.builder(client, consumer, new LongSerializer(), "/test").buildDelayQueue();
            queue.start();

            queue.put(1L, System.currentTimeMillis() + Integer.MAX_VALUE);  // never come out
            Long        value = consumer.take(1, TimeUnit.SECONDS);
            Assert.assertNull(value);

            queue.put(2L, System.currentTimeMillis());
            value = consumer.take(timing.seconds(), TimeUnit.SECONDS);
            Assert.assertEquals(value, Long.valueOf(2));

            value = consumer.take(1, TimeUnit.SECONDS);
            Assert.assertNull(value);
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        Timing                          timing = new Timing();
        DistributedDelayQueue<Long>     queue = null;
        CuratorFramework                client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            BlockingQueueConsumer<Long> consumer = new BlockingQueueConsumer<Long>(Mockito.mock(ConnectionStateListener.class));
            queue = QueueBuilder.builder(client, consumer, new LongSerializer(), "/test").buildDelayQueue();
            queue.start();

            queue.put(1L, System.currentTimeMillis() + 1000);
            Thread.sleep(100);
            Assert.assertEquals(consumer.size(), 0);    // delay hasn't been reached

            Long        value = consumer.take(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            Assert.assertEquals(value, Long.valueOf(1));
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        final int QTY = 10;

        Timing                          timing = new Timing();
        DistributedDelayQueue<Long>     queue = null;
        CuratorFramework                client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            BlockingQueueConsumer<Long> consumer = new BlockingQueueConsumer<Long>(Mockito.mock(ConnectionStateListener.class));
            queue = QueueBuilder.builder(client, consumer, new LongSerializer(), "/test").buildDelayQueue();
            queue.start();

            Random random = new Random();
            for ( int i = 0; i < 10; ++i )
            {
                long    delay = System.currentTimeMillis() + random.nextInt(100);
                queue.put(delay, delay);
            }

            long            lastValue = -1;
            for ( int i = 0; i < QTY; ++i )
            {
                Long        value = consumer.take(timing.forWaiting().seconds(), TimeUnit.SECONDS);
                Assert.assertNotNull(value);
                Assert.assertTrue(value >= lastValue);
                lastValue = value;
            }
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    private static class LongSerializer implements QueueSerializer<Long>
    {
        @Override
        public byte[] serialize(Long item)
        {
            return Long.toString(item).getBytes();
        }

        @Override
        public Long deserialize(byte[] bytes)
        {
            return Long.parseLong(new String(bytes));
        }
    }
}
