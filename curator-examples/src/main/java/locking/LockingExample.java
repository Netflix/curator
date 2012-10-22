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

package locking;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LockingExample
{
    private static final int        QTY = 5;
    private static final int        REPETITIONS = QTY * 10;

    private static final String     PATH = "/examples/locks";

    public static void main(String[] args) throws Exception
    {
        // all of the useful sample code is in ExampleClientThatLocks.java

        // FakeLimitedResource simulates some external resource that can only be access by one process at a time
        final FakeLimitedResource   resource = new FakeLimitedResource();

        ExecutorService             service = Executors.newFixedThreadPool(QTY);
        final TestingServer         server = new TestingServer();
        try
        {
            for ( int i = 0; i < QTY; ++i )
            {
                final int       index = i;
                Callable<Void>  task = new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
                        try
                        {
                            client.start();

                            ExampleClientThatLocks      example = new ExampleClientThatLocks(client, PATH, resource, "Client " + index);
                            for ( int j = 0; j < REPETITIONS; ++j )
                            {
                                example.doWork(10, TimeUnit.SECONDS);
                            }
                        }
                        catch ( Throwable e )
                        {
                            e.printStackTrace();
                        }
                        finally
                        {
                            Closeables.closeQuietly(client);
                        }
                        return null;
                    }
                };
                service.submit(task);
            }

            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);
        }
        finally
        {
            Closeables.closeQuietly(server);
        }
    }
}
