package com.netflix.curator.framework.recipes.nodes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.PathAndBytesable;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A persistent ephemeral node is an ephemeral node that attempts to stay present in ZooKeeper, even through connection
 * and session interruptions.
 */
public class PersistentEphemeralNode
{
    /** How long to wait for the node to be initially created in seconds. */
    private static final long CREATION_WAIT_IN_SECONDS = 10;

    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat(PersistentEphemeralNode.class.getSimpleName() + "Thread-%d")
            .setDaemon(true)
            .build();

    private final ScheduledExecutorService _executor;
    private final Async _async;
    private final AtomicBoolean _closed = new AtomicBoolean();

    /**
     * Create the ephemeral node in ZooKeeper.  If the node cannot be created in a timely fashion then an exception will
     * be thrown.
     */
    public PersistentEphemeralNode(CuratorFramework curator, String basePath, byte[] data, CreateMode mode)
    {
        checkNotNull(curator);
        checkArgument(curator.isStarted());
        checkNotNull(basePath);
        checkNotNull(data);
        checkNotNull(mode);
        checkArgument(mode == CreateMode.EPHEMERAL || mode == CreateMode.EPHEMERAL_SEQUENTIAL);

        // TODO: Share this executor across multiple nodes.  It *MUST* remain a single thread executor though and we
        // have to ensure that starvation doesn't ever happen for events on one node.
        _executor = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);

        _async = new Async(_executor, new Sync(curator, basePath, data, mode));

        CountDownLatch latch = new CountDownLatch(1);
        _async.createNode(latch);
        await(latch, CREATION_WAIT_IN_SECONDS, TimeUnit.SECONDS);
    }

    public void close(long duration, TimeUnit unit)
    {
        if ( !_closed.compareAndSet(false, true) )
        {
            // Already closed
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        _async.close(latch);
        await(latch, duration, unit);

        _executor.shutdown();
        await(_executor, duration, unit);
    }

    /**
     * Gets the actual path, including namespace (if any) and unique ID of the ZooKeeper node backing this object.
     * </p>
     * NOTE: This could potentially block forever (if the node never successfully gets created), so this method should
     * only be called in testing code.
     *
     * @return The actual path of the ZooKeeper node.
     * @throws InterruptedException If retrieval of the path is interrupted.
     * @throws java.util.concurrent.ExecutionException
     *                              Never.
     */
    @VisibleForTesting
    String getActualPath() throws ExecutionException, InterruptedException
    {
        return _async.getActualPath();
    }

    private void await(CountDownLatch latch, long duration, TimeUnit unit)
    {
        try
        {
            latch.await(duration, unit);
        } catch ( InterruptedException e )
        {
            throw Throwables.propagate(e);
        }
    }

    private void await(ExecutorService executor, long duration, TimeUnit unit)
    {
        try
        {
            executor.awaitTermination(duration, unit);
        } catch ( InterruptedException e )
        {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Watcher events are executed on the ZooKeeper event thread.  Switch over to the thread used by the methods
     * in the Sync class.
     */
    private class CheckExistsWatcher implements Watcher
    {
        private final AtomicBoolean _watcherCanceled;

        public CheckExistsWatcher(AtomicBoolean watcherCanceled)
        {
            _watcherCanceled = watcherCanceled;
        }

        @Override
        public void process(WatchedEvent event)
        {
            _async.onNodeChanged(_watcherCanceled, event);
        }
    }

    /**
     * Every method in the Async class is executed in a background thread so they will all return immediately.  All
     * operations proxy to the corresponding operation in the contained {@link Sync} object.
     */
    private static class Async
    {
        /** How long to wait (in the background) before attempting an asynchronous operation. */
        private static final long WAIT_DURATION_IN_MILLIS = 100;

        private final ScheduledExecutorService _executor;
        private final Sync _sync;

        private Async(ScheduledExecutorService executor, Sync sync)
        {
            _executor = executor;
            _sync = sync;
        }

        private void createNode(final CountDownLatch latch)
        {
            _executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    _sync.createNode(latch);
                }
            });
        }

        private void waitThenCreateNode(final CountDownLatch latch)
        {
            _executor.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    _sync.createNode(latch);
                }
            }, WAIT_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS);
        }

        private void waitThenWatchNode()
        {
            _executor.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    _sync.watchNode();
                }
            }, WAIT_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS);
        }

        private void waitThenDeleteNode(final CountDownLatch latch)
        {
            _executor.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    _sync.deleteNode(latch);
                }
            }, WAIT_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS);
        }

        private void onNodeChanged(final AtomicBoolean handled, final WatchedEvent event)
        {
            _executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    _sync.onNodeChanged(handled, event);
                }
            });
        }

        private void close(final CountDownLatch latch)
        {
            _executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    _sync.close(latch);
                }
            });
        }

        private String getActualPath() throws ExecutionException, InterruptedException
        {
            String path = _sync._nodePath;
            if ( path != null )
            {
                return path;
            }

            SettableFuture<String> future = SettableFuture.create();

            while ( !future.isDone() )
            {
                waitThenGetActualPath(future);
            }

            return future.get();
        }

        private void waitThenGetActualPath(final SettableFuture<String> future)
        {
            _executor.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    String path = _sync._nodePath;
                    if ( path != null )
                    {
                        future.set(path);
                    }
                }
            }, WAIT_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Every method in the Sync class is guaranteed to be executed on the same thread.  Because of this within Sync no
     * explicit synchronization is necessary.
     */
    private class Sync
    {
        private final CuratorFramework _curator;
        private final String _basePath;
        private final byte[] _data;

        private volatile String _nodePath;  // volatile since it may be read from other threads
        private boolean _closing;
        private boolean _deleted;

        // Store this at the class level because it encodes state that prevents the need for trying to create the path
        // multiple times.  If we instantiated this on the fly every time we tried to create a node we'd be wasting
        // effort since we'd know that that node was created already.
        private final EnsurePath _ensurePath;

        // Store this at the class level as well because it is a creation with protection so it has a UUID embedded in
        // the node name.  In order to ensure that that UUID remains constant for this ZooKeeperPersistentEphemeralNode
        // instance we need to only create this one time.
        private final PathAndBytesable<String> _createMethod;

        private Sync(CuratorFramework curator, String basePath, byte[] data, CreateMode mode)
        {
            _curator = curator;
            _basePath = basePath;
            _data = data;

            String parentDir = ZKPaths.getPathAndNode(_basePath).getPath();
            _ensurePath = _curator.newNamespaceAwareEnsurePath(parentDir);

            _createMethod = _curator.create().withProtection().withMode(mode);
        }

        private void createNode(CountDownLatch latch)
        {
            if ( _deleted )
            {
                return;
            }
            _nodePath = null;

            try
            {
                // Ensure the parents are created first...
                _ensurePath.ensure(_curator.getZookeeperClient());
            } catch ( Exception e )
            {
                _async.waitThenCreateNode(latch);
                return;
            }

            try
            {
                // Create the actual node...
                _nodePath = _createMethod.forPath(_basePath, _data);
            } catch ( KeeperException.NodeExistsException e )
            {
                // The node was already present, it may be created by us, maybe by another session.  In either
                // case we're going to start watching it and if it gets removed we'll recreate it under our session.
                _nodePath = e.getPath();
            } catch ( Exception e )
            {
                _async.waitThenCreateNode(latch);
                return;
            }

            watchNode();

            if ( latch != null )
            {
                latch.countDown();
            }
        }

        private void watchNode()
        {
            if ( _closing )
            {
                return;
            }

            // Use this to cancel the watcher when this method is going to do something that will eventually create
            // a new watcher.
            AtomicBoolean cancelWatcher = new AtomicBoolean();
            Stat stat;
            try
            {
                stat = _curator
                        .checkExists()
                        .usingWatcher(new CheckExistsWatcher(cancelWatcher))
                        .forPath(_nodePath);
            } catch ( Exception e )
            {
                cancelWatcher.set(true);
                _async.waitThenWatchNode();
                return;
            }

            if ( stat == null )
            {
                // The node didn't exist -- it needs to be created, but we've already registered a watcher.  Set the
                // watcher as handled so that when it's called later (when the node is created) it'll ignore that event.
                cancelWatcher.set(true);
                createNode(null);
            }
        }

        private void onNodeChanged(AtomicBoolean watcherCanceled, WatchedEvent event)
        {
            if ( _closing || !watcherCanceled.compareAndSet(false, true) )
            {
                return;
            }

            if ( event.getType() == Watcher.Event.EventType.NodeDeleted )
            {
                // Doesn't exist.  Must recreate it.
                createNode(null);
            } else if ( event.getType() == Watcher.Event.EventType.None )
            {
                // Something failed.  Try again in a little while.
                _async.waitThenWatchNode();
            } else
            {
                // Node changed in a way we don't care about.  Re-establish the watch.
                watchNode();
            }
        }

        private void deleteNode(CountDownLatch latch)
        {
            if ( _nodePath == null )
            {
                // The only time _nodePath is true is if we're creating a node.  Wait for it to finish.
                _async.waitThenDeleteNode(latch);
                return;
            }

            try
            {
                _curator.delete().forPath(_nodePath);
            } catch ( KeeperException.NoNodeException e )
            {
                // The node doesn't exist, we don't care, we're finished.
            } catch ( Exception e )
            {
                // Something failed.  Try again in a little while.
                _async.waitThenDeleteNode(latch);
                return;
            }

            _deleted = true;

            if ( latch != null )
            {
                latch.countDown();
            }
        }

        private void close(CountDownLatch latch)
        {
            if ( _closing ) return;

            _closing = true;
            deleteNode(latch);
        }
    }
}
