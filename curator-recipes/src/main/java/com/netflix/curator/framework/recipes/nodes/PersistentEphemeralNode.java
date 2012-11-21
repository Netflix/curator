package com.netflix.curator.framework.recipes.nodes;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.ACLBackgroundPathAndBytesable;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CreateModable;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.PathAndBytesable;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     A persistent ephemeral node is an ephemeral node that attempts to stay present in
 *     ZooKeeper, even through connection and session interruptions.
 * </p>
 *
 * <p>
 *     Thanks to <a href="http://github.com/bbeck">bbeck</a> and <a href="http://github.com/shawnsmith">shawnsmith</a>
 *     for the initial coding and design.
 * </p>
 */
public class PersistentEphemeralNode implements Closeable
{
    private volatile CountDownLatch         initialCreateLatch = new CountDownLatch(1);
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework          client;
    private final EnsurePath                ensurePath;
    private final PathAndBytesable<String>  createMethod;
    private final AtomicReference<String>   nodePath = new AtomicReference<String>(null);
    private final String                    basePath;
    private final Mode                      mode;
    private final byte[]                    data;
    private final AtomicReference<State>    state = new AtomicReference<State>(State.LATENT);
    private volatile boolean                isSuspended = false;
    private final Watcher                   watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            if ( Objects.equal(nodePath.get(), event.getPath()) )
            {
                createNode();
            }
        }
    };
    private final ConnectionStateListener   listener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            isSuspended = (newState != ConnectionState.RECONNECTED) && (newState != ConnectionState.CONNECTED);
            if ( newState == ConnectionState.RECONNECTED )
            {
                createNode();
            }
        }
    };
    private final BackgroundCallback        checkExistsCallback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
            {
                createNode();
            }
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * The mode for node creation
     */
    public enum Mode
    {
        /**
         * Same as {@link CreateMode#EPHEMERAL}
         */
        EPHEMERAL()
                {
                    @Override
                    protected CreateMode getCreateMode()
                    {
                        return CreateMode.EPHEMERAL;
                    }

                    @Override
                    protected boolean isProtected()
                    {
                        return false;
                    }

                    @Override
                    protected boolean isSequential()
                    {
                        return false;
                    }
                },

        /**
         * Same as {@link CreateMode#EPHEMERAL_SEQUENTIAL}
         */
        EPHEMERAL_SEQUENTIAL()
                {
                    @Override
                    protected CreateMode getCreateMode()
                    {
                        return CreateMode.EPHEMERAL_SEQUENTIAL;
                    }

                    @Override
                    protected boolean isProtected()
                    {
                        return false;
                    }

                    @Override
                    protected boolean isSequential()
                    {
                        return true;
                    }
                },

        /**
         * Same as {@link CreateMode#EPHEMERAL} with protection
         */
        PROTECTED_EPHEMERAL()
                {
                    @Override
                    protected CreateMode getCreateMode()
                    {
                        return CreateMode.EPHEMERAL;
                    }

                    @Override
                    protected boolean isProtected()
                    {
                        return true;
                    }

                    @Override
                    protected boolean isSequential()
                    {
                        return false;
                    }
                },

        /**
         * Same as {@link CreateMode#EPHEMERAL_SEQUENTIAL} with protection
         */
        PROTECTED_EPHEMERAL_SEQUENTIAL()
                {
                    @Override
                    protected CreateMode getCreateMode()
                    {
                        return CreateMode.EPHEMERAL_SEQUENTIAL;
                    }

                    @Override
                    protected boolean isProtected()
                    {
                        return true;
                    }

                    @Override
                    protected boolean isSequential()
                    {
                        return true;
                    }
                }

        ;

        protected abstract CreateMode getCreateMode();

        protected abstract boolean isProtected();

        protected abstract boolean isSequential();
    }

    /**
     * @param client client instance
     * @param mode creation/protection mode
     * @param basePath the base path for the node
     * @param data data for the node
     */
    public PersistentEphemeralNode(CuratorFramework client, Mode mode, String basePath, byte[] data)
    {
        this.client = Preconditions.checkNotNull(client, "client cannot be null");
        this.mode = Preconditions.checkNotNull(mode, "mode cannot be null");
        this.basePath = Preconditions.checkNotNull(basePath, "basePath cannot be null");
        this.data = Arrays.copyOf(Preconditions.checkNotNull(data, "data cannot be null"), data.length);

        String parentDir = ZKPaths.getPathAndNode(basePath).getPath();
        ensurePath = client.newNamespaceAwareEnsurePath(parentDir);

        BackgroundCallback        backgroundCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                String      path = null;
                if ( event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue() )
                {
                    path = event.getPath();
                }
                else if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    path = event.getName();
                }
                if ( path != null )
                {
                    nodePath.set(path);
                    watchNode();

                    if ( initialCreateLatch != null )
                    {
                        initialCreateLatch.countDown();
                        initialCreateLatch = null;
                    }

                    // If create hadn't finished but delete was called, but didn't know nodePath and thus couldn't
                    // delete the node, then we need to cleanup the node now.
                    if ( state.get() == State.CLOSED )
                    {
                        deleteNode();
                    }
                }
                else if ( state.get() == State.STARTED )
                {
                    createNode();
                }
            }
        };

        createMethod = makeCreateMethod(client, mode, backgroundCallback);
    }

    /**
     * You must call start() to initiate the persistent ephemeral node. An attempt to create the node
     * in the background will be started
     */
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        client.getConnectionStateListenable().addListener(listener);
        createNode();
    }

    /**
     * Block until the either initial node creation initiated by {@link #start()} succeeds or
     * the timeout elapses.
     *
     * @param timeout the maximum time to wait
     * @param unit time unit
     * @return if the node was created before timeout
     * @throws InterruptedException if the thread is interrupted
     */
    public boolean waitForInitialCreate(long timeout, TimeUnit unit) throws InterruptedException
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        CountDownLatch localInitialCreateLatch = initialCreateLatch;
        return localInitialCreateLatch == null || localInitialCreateLatch.await(timeout, unit);
    }

    /**
     * Close (and delete the node in the background)
     */
    @Override
    public void close()
    {
        if ( !state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            return;
        }

        client.getConnectionStateListenable().removeListener(listener);

        deleteNode();
    }

    /**
     * Returns the currently set path or null if the node does not exist
     *
     * @return node path or null
     */
    public String getActualPath()
    {
        return nodePath.get();
    }

    private void deleteNode()
    {
        String          localNodePath = nodePath.getAndSet(null);
        if ( localNodePath != null )
        {
            try
            {
                client.delete().guaranteed().inBackground().forPath(localNodePath);
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }
            catch ( Exception e )
            {
                log.error("Deleting node: " + localNodePath, e);
            }
        }
    }

    private void createNode()
    {
        if ( !isActive() )
        {
            return;
        }

        try
        {
            if ( mode.isSequential() )
            {
                deleteNode();
            }
            ensurePath.ensure(client.getZookeeperClient());
            createMethod.forPath(basePath, data);
        }
        catch ( Exception e )
        {
            log.error("Creating node. BasePath: " + basePath, e);
        }
    }

    private void watchNode()
    {
        if ( !isActive() )
        {
            return;
        }

        String          localNodePath = nodePath.get();
        if ( localNodePath != null )
        {
            try
            {
                client.checkExists().usingWatcher(watcher).inBackground(checkExistsCallback).forPath(localNodePath);
            }
            catch ( Exception e )
            {
                log.error("Watching node: " + localNodePath, e);
            }
        }
    }

    private boolean isActive()
    {
        return (state.get() == State.STARTED) && !isSuspended;
    }

    private static PathAndBytesable<String> makeCreateMethod(CuratorFramework curator, Mode mode, BackgroundCallback backgroundCallback)
    {
        CreateModable<ACLBackgroundPathAndBytesable<String>> builder = mode.isProtected() ? curator.create().withProtection() : curator.create();
        return builder.withMode(mode.getCreateMode()).inBackground(backgroundCallback);
    }
}