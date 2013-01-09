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

import com.netflix.curator.framework.state.ConnectionState;

import java.util.Collection;

/**
 * POJO that abstracts a change to a path
 */
public class PathChildrenCacheEvent
{
    private final Type type;
    private final ChildData child;
    private final Collection<ChildData> children;

    /**
     * Type of change
     */
    public enum Type
    {
        /**
         * The cache has been populated with an initial set of children
         */
        CHILDREN_INITIALIZED,

        /**
         * A child was added to the path
         */
        CHILD_ADDED,

        /**
         * A child's data was changed
         */
        CHILD_UPDATED,

        /**
         * A child was removed from the path
         */
        CHILD_REMOVED,

        /**
         * Called when the connection has changed to {@link ConnectionState#SUSPENDED}
         */
        CONNECTION_SUSPENDED,

        /**
         * Called when the connection has changed to {@link ConnectionState#RECONNECTED}
         */
        CONNECTION_RECONNECTED,

        /**
         * Called when the connection has changed to {@link ConnectionState#LOST}
         */
        CONNECTION_LOST
    }

    /**
     * @param type event type
     * @param child event child data or null
     * @param children event children data or null
     */
    public PathChildrenCacheEvent(Type type, ChildData child, Collection<ChildData> children)
    {
        this.type = type;
        this.child = child;
        this.children = children;
    }

    /**
     * @return change type
     */
    public Type getType()
    {
        return type;
    }

    /**
     * @return the child data
     */
    public ChildData getChild()
    {
        return child;
    }

    /**
     * @return the children data
     */
    public Collection<ChildData> getChildren()
    {
        return children;
    }

    @Override
    public String toString()
    {
        return "PathChildrenCacheEvent{" +
            "type=" + type +
            ", child=" + child +
            ", children=" + children +
            '}';
    }
}
