/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.recipes.queue;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorEvent;
import com.netflix.curator.framework.CuratorEventType;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorListener;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>An implementation of the Distributed Queue ZK recipe. Items put into the queue
 * are guaranteed to be ordered (by means of ZK's PERSISTENT_SEQUENTIAL node).</p>
 *
 * <p>
 * Guarantees:<br/>
 * <li>If a single consumer takes items out of the queue, they will be ordered FIFO. i.e. if ordering is important,
 * use a {@link LeaderSelector} to nominate a single consumer.</li>
 * <li>There is guaranteed processing of each message to the point of receipt by a given instance.
 * If an instance receives an item from the queue but dies while processing it, the item will be lost</li>
 * <li><b>IMPORTANT: </b>If you use a number greater than 1 for <code>maxInternalQueue</code> when
 * constructing a <code>DistributedQueue</code>, you risk that many items getting lost if your process
 * dies without processing them. i.e. <code>maxInternalQueue</code> items get taken out of the distributed
 * queue and placed in your local instance's in-memory queue.</li>
 * </p>
 */
public class DistributedQueue<T> implements Closeable
{
    private final CuratorFramework client;
    private final QueueSerializer<T> serializer;
    private final String queuePath;
    private final Executor executor;
    private final BlockingQueue<T> itemQueue;
    private final ExecutorService service;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final AtomicReference<Exception> backgroundException = new AtomicReference<Exception>(null);
    private final boolean refreshOnWatch;
    private final AtomicBoolean refreshOnWatchSignaled = new AtomicBoolean(false);
    private final CuratorListener listener = new CuratorListener()
    {
        @Override
        public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
        {
            if ( event.getType() == CuratorEventType.WATCHED )
            {
                if ( event.getWatchedEvent().getType() == Watcher.Event.EventType.NodeChildrenChanged )
                {
                    internalNotify();
                }
            }
        }

        @Override
        public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
        {
            // nop
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    private static final String     QUEUE_ITEM_NAME = "queue-";

    /**
     * @param client the client
     * @param serializer serializer for in/out data
     * @param queuePath the ZK path to use for the queue
     */
    public DistributedQueue(CuratorFramework client, QueueSerializer<T> serializer, String queuePath)
    {
        this(client, serializer, queuePath, Executors.defaultThreadFactory(), MoreExecutors.sameThreadExecutor(), Integer.MAX_VALUE, false);
    }

    /**
     * @param client the client
     * @param serializer serializer for in/out data
     * @param queuePath the ZK path to use for the queue
     * @param threadFactory factory to use for making internal threads
     * @param executor the executor to run in
     * @param maxInternalQueue queue items are taken from ZK and stored in an internal Java queue. maxInternalQueue specifies the
     * max length for that queue. When full, new items will block until there's room in the queue
     */
    public DistributedQueue(CuratorFramework client, QueueSerializer<T> serializer, String queuePath, ThreadFactory threadFactory, Executor executor, int maxInternalQueue)
    {
        this(client, serializer, queuePath, threadFactory, executor, maxInternalQueue, false);
    }

    DistributedQueue(CuratorFramework client, QueueSerializer<T> serializer, String queuePath, ThreadFactory threadFactory, Executor executor, int maxInternalQueue, boolean refreshOnWatch)
    {
        this.refreshOnWatch = refreshOnWatch;
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(queuePath);
        Preconditions.checkNotNull(threadFactory);
        Preconditions.checkNotNull(executor);
        Preconditions.checkArgument(maxInternalQueue >= 0);

        this.client = client;
        this.serializer = serializer;
        this.queuePath = queuePath;
        this.executor = executor;
        itemQueue = (maxInternalQueue == 0) ? new SynchronousQueue<T>() : new LinkedBlockingQueue<T>(maxInternalQueue);
        service = Executors.newSingleThreadExecutor(threadFactory);
    }

    /**
     * Start the queue. No other methods work until this is called
     *
     * @throws Exception startup errors
     */
    public void     start() throws Exception
    {
        if ( !state.compareAndSet(State.LATENT, State.STARTED) )
        {
            throw new IllegalStateException();
        }

        try
        {
            client.create().creatingParentsIfNeeded().forPath(queuePath, new byte[0]);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // this is OK
        }

        client.addListener(listener, executor);
        service.submit
        (
            new Callable<Object>()
            {
                @Override
                public Object call()
                {
                    runLoop();
                    return null;
                }
            }
        );
    }

    @Override
    public void close() throws IOException
    {
        if ( !state.compareAndSet(State.STARTED, State.STOPPED) )
        {
            throw new IllegalStateException();
        }

        client.removeListener(listener);
        service.shutdownNow();
    }

    /**
     * Add an item into the queue. Adding is done in the background - thus, this method will
     * return quickly.
     *
     * @param item item to add
     * @throws Exception connection issues
     */
    public void     put(T item) throws Exception
    {
        checkState();

        String      path = makeItemPath();
        internalPut(item, path);
    }

    /**
     * Take the next item off of the queue blocking until there is an item available
     *
     * @return the item
     * @throws Exception thread interruption or an error in the background thread
     */
    public T        take() throws Exception
    {
        checkState();

        return itemQueue.take();
    }

    /**
     * Take the next item off of the queue blocking until there is an item available
     * or the specified timeout has elapsed
     *
     * @param timeout timeout
     * @param unit unit
     * @return the item or null if timed out
     * @throws Exception thread interruption or an error in the background thread
     */
    public T        take(long timeout, TimeUnit unit) throws Exception
    {
        checkState();

        return itemQueue.poll(timeout, unit);
    }

    /**
     * Return the number of pending items in the local Java queue. IMPORTANT: when this method
     * returns a non-zero value, there is no guarantee that a subsequent call to take() will not
     * block. i.e. items can get removed between this method call and others.
     *
     * @return item qty or 0
     * @throws Exception an error in the background thread
     */
    public int      available() throws Exception
    {
        checkState();

        return itemQueue.size();
    }

    void internalPut(T item, String path) throws Exception
    {
        byte[]      bytes = serializer.serialize(item);
        client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).inBackground().forPath(path, bytes);
    }

    void checkState() throws Exception
    {
        if ( state.get() != State.STARTED )
        {
            throw new IllegalStateException();
        }

        Exception exception = backgroundException.getAndSet(null);
        if ( exception != null )
        {
            throw exception;
        }
    }

    String makeItemPath()
    {
        return ZKPaths.makePath(queuePath, QUEUE_ITEM_NAME);
    }

    private synchronized void internalNotify()
    {
        if ( refreshOnWatch )
        {
            refreshOnWatchSignaled.set(true);
        }
        notifyAll();
    }

    private void runLoop()
    {
        try
        {
            while ( !Thread.currentThread().isInterrupted()  )
            {
                List<String>        children;
                synchronized(this)
                {
                    do
                    {
                        children = client.getChildren().watched().forPath(queuePath);
                        if ( children.size() == 0 )
                        {
                            wait();
                        }
                    } while ( children.size() == 0 );

                    refreshOnWatchSignaled.set(false);
                }
                if ( children.size() > 0 )
                {
                    processChildren(children);
                }
            }
        }
        catch ( Exception e )
        {
            backgroundException.set(e);
        }
    }

    private void processChildren(List<String> children) throws Exception
    {
        Collections.sort(children); // makes sure items are processed in the order they were added
        for ( String itemNode : children )
        {
            if ( refreshOnWatchSignaled.compareAndSet(true, false) || Thread.currentThread().isInterrupted() )
            {
                break;
            }

            String  itemPath = ZKPaths.makePath(queuePath, itemNode);
            try
            {
                Stat    stat = new Stat();
                byte[]  bytes = client.getData().storingStatIn(stat).forPath(itemPath);
                client.delete().withVersion(stat.getVersion()).forPath(itemPath);

                T       item = serializer.deserialize(bytes);
                itemQueue.put(item);
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // another process got it
            }
            catch ( KeeperException.BadVersionException ignore )
            {
                // another process got it
            }
        }
    }
}
