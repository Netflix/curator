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
package com.netflix.curator.framework.recipes.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ThreadUtils;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 *
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 */
public class PathChildrenCache implements Closeable
{
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework          client;
    private final String                    path;
    private final ExecutorService           executorService;
    private final boolean                   cacheData;
    private final boolean                   dataIsCompressed;
    private final EnsurePath                ensurePath;
    private final BlockingQueue<Operation>                      operations = new LinkedBlockingQueue<Operation>();
    private final ListenerContainer<PathChildrenCacheListener>  listeners = new ListenerContainer<PathChildrenCacheListener>();
    private final ConcurrentMap<String, ChildData>              currentData = Maps.newConcurrentMap();

    private final Watcher     childrenWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            offerOperation(new RefreshOperation(PathChildrenCache.this));
        }
    };

    private final Watcher     dataWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try
            {
                if ( event.getType() == Event.EventType.NodeDeleted )
                {
                    remove(event.getPath());
                }
                else if ( event.getType() == Event.EventType.NodeDataChanged )
                {
                    offerOperation(new GetDataOperation(PathChildrenCache.this, event.getPath()));
                }
            }
            catch ( Exception e )
            {
                handleException(e);
            }
        }
    };

    @VisibleForTesting
    volatile Exchanger<Object>      rebuildTestExchanger;

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };
    private static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("PathChildrenCache");

    /**
     * @param client the client
     * @param path path to watch
     * @param mode caching mode
     *
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean)} instead
     */
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, defaultThreadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param mode caching mode
     * @param threadFactory factory to use when creating internal threads
     *
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean, ThreadFactory)} instead
     */
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode, ThreadFactory threadFactory)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, threadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)
    {
        this(client, path, cacheData, false, defaultThreadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, ThreadFactory threadFactory)
    {
        this(client, path, cacheData, false, threadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, ThreadFactory threadFactory)
    {
        this.client = client;
        this.path = path;
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        executorService = Executors.newFixedThreadPool(1, threadFactory);
        ensurePath = client.newNamespaceAwareEnsurePath(path);
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void     start() throws Exception
    {
        start(false);
    }

    /**
     * Same as {@link #start()} but gives the option of doing an initial build
     *
     * @param buildInitial if true, {@link #rebuild()} will be called before this method
     *                     returns in order to get an initial view of the node; otherwise,
     *                     the cache will be initialized asynchronously
     * @throws Exception errors
     */
    public void     start(boolean buildInitial) throws Exception
    {
        Preconditions.checkState(!executorService.isShutdown(), "already started");

        client.getConnectionStateListenable().addListener(connectionStateListener);
        executorService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        mainLoop();
                        return null;
                    }
                }
            );

        if ( buildInitial )
        {
            rebuild();
        }
        else
        {
            offerOperation(new InitializeOperation(this));
        }
    }

    /**
     * NOTE: this is a BLOCKING method. Completely rebuild the internal cache by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @throws Exception errors
     */
    public void     rebuild() throws Exception
    {
        Preconditions.checkState(!executorService.isShutdown(), "cache has been closed");

        ensurePath.ensure(client.getZookeeperClient());

        List<String>            children = client.getChildren().forPath(path);
        for ( String child : children )
        {
            String  fullPath = ZKPaths.makePath(path, child);
            internalRebuildNode(fullPath);

            if ( rebuildTestExchanger != null )
            {
                rebuildTestExchanger.exchange(new Object());
            }
        }

        // this is necessary so that any updates that occurred while rebuilding are taken
        offerOperation(new ForceRefreshOperation(this));
    }

    /**
     * NOTE: this is a BLOCKING method. Rebuild the internal cache for the given node by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @param fullPath full path of the node to rebuild
     * @throws Exception errors
     */
    public void     rebuildNode(String fullPath) throws Exception
    {
        Preconditions.checkArgument(ZKPaths.getPathAndNode(fullPath).getPath().equals(path), "Node is not part of this cache: " + fullPath);
        Preconditions.checkState(!executorService.isShutdown(), "cache has been closed");

        ensurePath.ensure(client.getZookeeperClient());
        internalRebuildNode(fullPath);

        // this is necessary so that any updates that occurred while rebuilding are taken
        // have to rebuild entire tree in case this node got deleted in the interim
        offerOperation(new ForceRefreshOperation(this));
    }

    /**
     * Close/end the cache
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(!executorService.isShutdown(), "has not been started");

        client.getConnectionStateListenable().removeListener(connectionStateListener);
        executorService.shutdownNow();
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public ListenerContainer<PathChildrenCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    public List<ChildData>      getCurrentData()
    {
        return ImmutableList.copyOf(Sets.<ChildData>newTreeSet(currentData.values()));
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no child with that path, <code>null</code>
     * is returned.
     *
     * @param fullPath full path to the node to check
     * @return data or null
     */
    public ChildData            getCurrentData(String fullPath)
    {
        return currentData.get(fullPath);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath the path of the node to clear
     */
    public void         clearDataBytes(String fullPath)
    {
        clearDataBytes(fullPath, -1);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath the path of the node to clear
     * @param ifVersion if non-negative, only clear the data if the data's version matches this version
     * @return true if the data was cleared
     */
    public boolean         clearDataBytes(String fullPath, int ifVersion)
    {
        ChildData data = currentData.get(fullPath);
        if ( data != null )
        {
            if ( (ifVersion < 0) || (ifVersion == data.getStat().getVersion()) )
            {
                data.clearData();
                return true;
            }
        }
        return false;
    }

    /**
     * Clear out current data and begin a new query on the path
     *
     * @throws Exception errors
     */
    public void clearAndRefresh() throws Exception
    {
        currentData.clear();
        offerOperation(new RefreshOperation(this));
    }

    /**
     * Clears the current data without beginning a new query and without generating any events
     * for listeners.
     */
    public void clear()
    {
        currentData.clear();
    }

    void initialize()
    {
        Collection<ChildData> newChildDatas = new ArrayList<ChildData>();
        Set<ChildData> childDatasToRemove = new HashSet<ChildData>(currentData.values());

        try
        {
            ensurePath.ensure(client.getZookeeperClient());

            List<String>            children = client.getChildren().forPath(path);
            for ( String child : children )
            {
                String  fullPath = ZKPaths.makePath(path, child);

                // add or update the child
                InternalRebuildNodeResult result = internalRebuildNode(fullPath);
                newChildDatas.add(result.newChild);

                // fire the add or update event
                if (result.getOldChild() == null)
                {
                    offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, result.getNewChild(), null)));
                }
                else
                {
                    offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, result.getNewChild(), null)));
                }
            }

            // remove any children that should no longer be in the cache, then fire the remove events
            for (ChildData newChildData : newChildDatas)
            {
                childDatasToRemove.remove(ZKPaths.makePath(path, newChildData.getPath()));
            }

            for (ChildData childToRemove : childDatasToRemove)
            {
                currentData.remove(ZKPaths.makePath(path, childToRemove.getPath()));

                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, childToRemove, null)));
            }
        }
        catch (Throwable t)
        {
            handleException(t);

            // try again
            offerOperation(new InitializeOperation(this));

            return;
        }

        // fire the initialized event
        offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILDREN_INITIALIZED, null, newChildDatas)));

        // this is necessary so that any updates that occurred while initializing are taken
        offerOperation(new ForceRefreshOperation(this));
    }

    void refresh(final boolean forceGetDataAndStat) throws Exception
    {
        ensurePath.ensure(client.getZookeeperClient());

        final BackgroundCallback  callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                processChildren(event.getChildren(), forceGetDataAndStat);
            }
        };

        client.getChildren().usingWatcher(childrenWatcher).inBackground(callback).forPath(path);
    }

    void callListeners(final PathChildrenCacheEvent event)
    {
        listeners.forEach
            (
                new Function<PathChildrenCacheListener, Void>()
                {
                    @Override
                    public Void apply(PathChildrenCacheListener listener)
                    {
                        try
                        {
                            listener.childEvent(client, event);
                        }
                        catch ( Exception e )
                        {
                            handleException(e);
                        }
                        return null;
                    }
                }
            );
    }

    void getDataAndStat(final String fullPath) throws Exception
    {
        BackgroundCallback  existsCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                applyNewData(fullPath, event.getResultCode(), event.getStat(), null);
            }
        };

        BackgroundCallback  getDataCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                applyNewData(fullPath, event.getResultCode(), event.getStat(), event.getData());
            }
        };

        if ( cacheData )
        {
            if ( dataIsCompressed )
            {
                client.getData().decompressed().usingWatcher(dataWatcher).inBackground(getDataCallback).forPath(fullPath);
            }
            else
            {
                client.getData().usingWatcher(dataWatcher).inBackground(getDataCallback).forPath(fullPath);
            }
        }
        else
        {
            client.checkExists().usingWatcher(dataWatcher).inBackground(existsCallback).forPath(fullPath);
        }
    }

    /**
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void      handleException(Throwable e)
    {
        log.error("", e);
    }

    private static class InternalRebuildNodeResult
    {
        private ChildData oldChild;
        private ChildData newChild;

        public InternalRebuildNodeResult(ChildData oldChild, ChildData newChild)
        {
            this.oldChild = oldChild;
            this.newChild = newChild;
        }

        public ChildData getOldChild()
        {
            return oldChild;
        }

        public ChildData getNewChild()
        {
            return newChild;
        }
    }

    private InternalRebuildNodeResult internalRebuildNode(String fullPath) throws Exception
    {
        ChildData newChildData = null;
        ChildData oldChildData = null;

        if ( cacheData )
        {
            try
            {
                Stat stat = new Stat();
                byte[]      bytes = dataIsCompressed ? client.getData().decompressed().storingStatIn(stat).forPath(fullPath) : client.getData().storingStatIn(stat).forPath(fullPath);
                newChildData = new ChildData(fullPath, stat, bytes);
                oldChildData = currentData.put(fullPath, newChildData);
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }
        }
        else
        {
            Stat        stat = client.checkExists().forPath(fullPath);
            if ( stat != null )
            {
                newChildData = new ChildData(fullPath, stat, null);
                oldChildData = currentData.put(fullPath, newChildData);
            }
        }

        return new InternalRebuildNodeResult(oldChildData, newChildData);
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
        case SUSPENDED:
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED, null, null)));
            break;
        }

        case LOST:
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_LOST, null, null)));
            break;
        }

        case RECONNECTED:
        {
            try
            {
                offerOperation(new ForceRefreshOperation(this));
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED, null, null)));
            }
            catch ( Exception e )
            {
                handleException(e);
            }
            break;
        }
        }
    }

    private void processChildren(List<String> children, boolean forceGetDataAndStat) throws Exception
    {
        List<String>    fullPaths = Lists.transform
        (
            children,
            new Function<String, String>()
            {
                @Override
                public String apply(String child)
                {
                    return ZKPaths.makePath(path, child);
                }
            }
        );
        Set<String>     removedNodes = Sets.newHashSet(currentData.keySet());
        removedNodes.removeAll(fullPaths);

        for ( String fullPath : removedNodes )
        {
            remove(fullPath);
        }

        for ( String name : children )
        {
            String      fullPath = ZKPaths.makePath(path, name);
            if ( forceGetDataAndStat || !currentData.containsKey(fullPath) )
            {
                getDataAndStat(fullPath);
            }
        }
    }

    private void remove(String fullPath)
    {
        ChildData data = currentData.remove(fullPath);
        if ( data != null )
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, data, null)));
        }
    }

    private void applyNewData(String fullPath, int resultCode, Stat stat, byte[] bytes)
    {
        if ( resultCode == KeeperException.Code.OK.intValue() ) // otherwise - node must have dropped or something - we should be getting another event
        {
            ChildData       data = new ChildData(fullPath, stat, bytes);
            ChildData       previousData = currentData.put(fullPath, data);
            if ( previousData == null ) // i.e. new
            {
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, data, null)));
            }
            else if ( previousData.getStat().getVersion() != stat.getVersion() )
            {
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data, null)));
            }
        }
    }

    private void mainLoop()
    {
        while ( !Thread.currentThread().isInterrupted() )
        {
            try
            {
                operations.take().invoke();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
            catch ( Exception e )
            {
                handleException(e);
            }
        }
    }

    private void offerOperation(Operation operation)
    {
        operations.remove(operation);   // avoids herding for refresh operations
        operations.offer(operation);
    }
}
