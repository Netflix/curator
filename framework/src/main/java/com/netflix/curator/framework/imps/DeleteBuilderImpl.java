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
package com.netflix.curator.framework.imps;

import com.netflix.curator.RetryLoop;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.api.*;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.BackgroundPathable;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.DeleteBuilder;
import org.apache.zookeeper.AsyncCallback;
import java.util.concurrent.Executor;

class DeleteBuilderImpl implements DeleteBuilder, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl client;
    private int                                     version;
    private Backgrounding backgrounding;

    DeleteBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        version = -1;
        backgrounding = new Backgrounding();
    }

    @Override
    public BackgroundPathable<Void> withVersion(int version)
    {
        this.version = version;
        return this;
    }

    @Override
    public Pathable<Void> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public Pathable<Void> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public Pathable<Void> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public Pathable<Void> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception
    {
        final TimeTrace   trace = client.getZookeeperClient().startTracer("DeleteBuilderImpl-Background");
        client.getZooKeeper().delete
        (
            operationAndData.getData(),
            version,
            new AsyncCallback.VoidCallback()
            {
                @Override
                public void processResult(int rc, String path, Object ctx)
                {
                    trace.commit();
                    CuratorEvent event = new CuratorEventImpl(CuratorEventType.DELETE, rc, path, null, ctx, null, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            },
            backgrounding.getContext()
        );
    }

    @Override
    public Void forPath(String path) throws Exception
    {
        path = client.fixForNamespace(path);

        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback()), null);
        }
        else
        {
            pathInForeground(path);
        }
        return null;
    }

    private void pathInForeground(String path) throws Exception
    {
        TimeTrace trace = client.getZookeeperClient().startTracer("DeleteBuilderImpl-Foreground");
        RetryLoop retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                client.getZooKeeper().delete(path, version);
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
        }
        trace.commit();
    }
}
