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

package com.netflix.curator.test;

import com.google.common.collect.ImmutableList;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.apache.zookeeper.ZooKeeper;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

/**
 * manages an internally running ensemble of ZooKeeper servers. FOR TESTING PURPOSES ONLY
 */
public class TestingCluster implements Closeable
{
    static
    {
        ByteCodeRewrite.apply();
    }

    private final QuorumConfigBuilder           builder;
    private final List<TestingZooKeeperServer>  servers;

    /**
     * Creates an ensemble comprised of <code>n</code> servers. Each server will use
     * a temp directory and random ports
     *
     * @param instanceQty number of servers to create in the ensemble
     */
    public TestingCluster(int instanceQty)
    {
        this(makeSpecs(instanceQty));
    }

    /**
     * Creates an ensemble using the given server specs
     *
     * @param specs the server specs
     */
    public TestingCluster(InstanceSpec... specs)
    {
        this(ImmutableList.copyOf(specs));
    }

    /**
     * Creates an ensemble using the given server specs
     *
     * @param specs the server specs
     */
    public TestingCluster(Collection<InstanceSpec> specs)
    {
        builder = new QuorumConfigBuilder(specs);
        ImmutableList.Builder<TestingZooKeeperServer> serverBuilder = ImmutableList.builder();
        for ( int i = 0; i < specs.size(); ++i )
        {
            serverBuilder.add(new TestingZooKeeperServer(builder, i));
        }
        servers = serverBuilder.build();
    }

    /**
     * Returns the set of servers in the ensemble
     *
     * @return set of servers
     */
    public Collection<InstanceSpec> getInstances()
    {
        return builder.getInstanceSpecs();
    }

    /**
     * Returns the connection string to pass to the ZooKeeper constructor
     *
     * @return connection string
     */
    public String   getConnectString()
    {
        StringBuilder       str = new StringBuilder();
        for ( InstanceSpec spec : builder.getInstanceSpecs() )
        {
            if ( str.length() > 0 )
            {
                str.append(",");
            }
            str.append(spec.getConnectString());
        }
        return str.toString();
    }

    /**
     * Start the ensemble. The cluster must be started before use.
     *
     * @throws Exception errors
     */
    public void     start() throws Exception
    {
        for ( TestingZooKeeperServer server : servers )
        {
            server.start();
        }
    }

    /**
     * Shutdown the ensemble WITHOUT freeing resources, etc.
     */
    public void stop() throws IOException
    {
        for ( TestingZooKeeperServer server : servers )
        {
            server.stop();
        }
    }

    /**
     * Shutdown the ensemble, free resources, etc. If temp directories were used, they
     * are deleted. You should call this in a <code>finally</code> block.
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        for ( TestingZooKeeperServer server : servers )
        {
            server.close();
        }
    }

    /**
     * Kills the given server. This simulates the server unexpectedly crashing
     *
     * @param instance server to kill
     * @throws Exception errors
     * @return true if the instance was found
     */
    public boolean killServer(InstanceSpec instance) throws Exception
    {
        for ( TestingZooKeeperServer server : servers )
        {
            if ( server.getInstanceSpec().equals(instance) )
            {
                server.kill();
                return true;
            }
        }
        return false;
    }

    /**
     * Given a ZooKeeper instance, returns which server it is connected to
     *
     * @param client ZK instance
     * @return the server
     * @throws Exception errors
     */
    public InstanceSpec findConnectionInstance(ZooKeeper client) throws Exception
    {
        Method              m = client.getClass().getDeclaredMethod("testableRemoteSocketAddress");
        m.setAccessible(true);
        InetSocketAddress   address = (InetSocketAddress)m.invoke(client);
        if ( address != null )
        {
            for ( TestingZooKeeperServer server : servers )
            {
                if ( server.getInstanceSpec().getPort() == address.getPort() )
                {
                    return server.getInstanceSpec();
                }
            }
        }

        return null;
    }

    private static Collection<InstanceSpec> makeSpecs(int instanceQty)
    {
        ImmutableList.Builder<InstanceSpec> builder = ImmutableList.builder();
        for ( int i = 0; i < instanceQty; ++i )
        {
            builder.add(InstanceSpec.newInstanceSpec());
        }
        return builder.build();
    }
}
