/*
 * Copyright 2013 Netflix, Inc.
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

package com.netflix.curator.framework.imps;

import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;

class NamespaceImpl
{
    private final CuratorFrameworkImpl client;
    private final String namespace;
    private final EnsurePath ensurePath;

    NamespaceImpl(CuratorFrameworkImpl client, String namespace)
    {
        this.client = client;
        this.namespace = namespace;
        ensurePath = (namespace != null) ? new EnsurePath(ZKPaths.makePath("/", namespace)) : null;
    }

    String getNamespace()
    {
        return namespace;
    }

    String    unfixForNamespace(String path)
    {
        if ( (namespace != null) && (path != null) )
        {
            String      namespacePath = ZKPaths.makePath(namespace, null);
            if ( path.startsWith(namespacePath) )
            {
                path = (path.length() > namespacePath.length()) ? path.substring(namespacePath.length()) : "/";
            }
        }
        return path;
    }

    String    fixForNamespace(String path)
    {
        if ( ensurePath != null )
        {
            try
            {
                ensurePath.ensure(client.getZookeeperClient());
            }
            catch ( Exception e )
            {
                client.logError("Ensure path threw exception", e);
            }
        }

        return ZKPaths.fixForNamespace(namespace, path);
    }

    EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return new EnsurePath(fixForNamespace(path));
    }
}
