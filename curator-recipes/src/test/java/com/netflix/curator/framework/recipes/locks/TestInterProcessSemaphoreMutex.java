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
package com.netflix.curator.framework.recipes.locks;

import com.netflix.curator.framework.CuratorFramework;
import org.testng.annotations.Test;

public class TestInterProcessSemaphoreMutex extends TestInterProcessMutexBase
{
    private static final String LOCK_PATH = "/locks/our-lock";

    @Override
    @Test(enabled = false)
    public void testReentrant() throws Exception
    {
    }

    @Override
    @Test(enabled = false)
    public void testReentrant2Threads() throws Exception
    {
    }

    @Override
    protected InterProcessLock makeLock(CuratorFramework client)
    {
        return new InterProcessSemaphoreMutex(client, LOCK_PATH);
    }
}
