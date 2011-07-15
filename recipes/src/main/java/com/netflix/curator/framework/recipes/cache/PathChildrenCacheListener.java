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
package com.netflix.curator.framework.recipes.cache;

import com.netflix.curator.framework.CuratorFramework;

/**
 * Listener for PathChildrenCache changes
 */
public interface PathChildrenCacheListener
{
    /**
     * Called when a change has occurred
     *
     * @param client the client
     * @param event describes the change
     * @throws Exception errors
     */
    public void     childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception;

    /**
     * Called when an exception that can't be processed internally is caught
     *
     * @param client the client
     * @param exception the exception
     */
    public void     handleException(CuratorFramework client, Exception exception);

    /**
     * Will get called if client connection unexpectedly closes
     *
     * @param client the client
     */
    public void     notifyClientClosing(CuratorFramework client);
}
