/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.framework.imps;

import com.netflix.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FailedDeleteManager
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFrameworkImpl client;

    FailedDeleteManager(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    void addFailedDelete(String fixedPath)
    {
        String unfixedPath = client.unfixForNamespace(fixedPath);

        if ( client.isStarted() )
        {
            log.debug("Path being added to guaranteed delete set: " + fixedPath);
            try
            {
                client.delete().guaranteed().inBackground().forPath(unfixedPath);
            }
            catch ( Exception e )
            {
                addFailedDelete(fixedPath);
            }
        }
    }
}
