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
package com.netflix.curator.framework;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.ensemble.EnsembleProvider;
import com.netflix.curator.ensemble.fixed.FixedEnsembleProvider;
import com.netflix.curator.framework.api.ACLProvider;
import com.netflix.curator.framework.api.CompressionProvider;
import com.netflix.curator.framework.api.PathAndBytesable;
import com.netflix.curator.framework.imps.CuratorFrameworkImpl;
import com.netflix.curator.framework.imps.DefaultACLProvider;
import com.netflix.curator.framework.imps.GzipCompressionProvider;
import com.netflix.curator.utils.DefaultZookeeperFactory;
import com.netflix.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;

/**
 * Factory methods for creating framework-style clients
 */
public class CuratorFrameworkFactory
{
    private static final int        DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);
    private static final int        DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

    private static final byte[]     LOCAL_ADDRESS = getLocalAddress();

    private static final CompressionProvider        DEFAULT_COMPRESSION_PROVIDER = new GzipCompressionProvider();
    private static final DefaultZookeeperFactory    DEFAULT_ZOOKEEPER_FACTORY = new DefaultZookeeperFactory();
    private static final DefaultACLProvider         DEFAULT_ACL_PROVIDER = new DefaultACLProvider();

    /**
     * Return a new builder that builds a CuratorFramework
     *
     * @return new builder
     */
    public static Builder       builder()
    {
        return new Builder();
    }

    /**
     * Create a new client with default session timeout and default connection timeout
     *
     *
     * @param connectString list of servers to connect to
     * @param retryPolicy retry policy to use
     * @return client
     */
    public static CuratorFramework newClient(String connectString, RetryPolicy retryPolicy)
    {
        return newClient(connectString, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, retryPolicy);
    }

    /**
     * Create a new client
     *
     *
     * @param connectString list of servers to connect to
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param retryPolicy retry policy to use
     * @return client
     */
    public static CuratorFramework newClient(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy)
    {
        return builder().
            connectString(connectString).
            sessionTimeoutMs(sessionTimeoutMs).
            connectionTimeoutMs(connectionTimeoutMs).
            retryPolicy(retryPolicy).
            build();
    }

    public static class Builder
    {
        private EnsembleProvider    ensembleProvider;
        private int                 sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
        private int                 connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
        private RetryPolicy         retryPolicy;
        private ThreadFactory       threadFactory = null;
        private String              namespace;
        private String              authScheme = null;
        private byte[]              authValue = null;
        private byte[]              defaultData = LOCAL_ADDRESS;
        private CompressionProvider compressionProvider = DEFAULT_COMPRESSION_PROVIDER;
        private ZookeeperFactory    zookeeperFactory = DEFAULT_ZOOKEEPER_FACTORY;
        private ACLProvider         aclProvider = DEFAULT_ACL_PROVIDER;
        private boolean             canBeReadOnly = false;

        /**
         * Apply the current values and build a new CuratorFramework
         *
         * @return new CuratorFramework
         */
        public CuratorFramework build()
        {
            return new CuratorFrameworkImpl(this);
        }

        /**
         * Add connection authorization
         *
         * @param scheme the scheme
         * @param auth the auth bytes
         * @return this
         */
        public Builder  authorization(String scheme, byte[] auth)
        {
            this.authScheme = scheme;
            this.authValue = (auth != null) ? Arrays.copyOf(auth, auth.length) : null;
            return this;
        }

        /**
         * Set the list of servers to connect to. IMPORTANT: use either this or {@link #ensembleProvider(EnsembleProvider)}
         * but not both.
         *
         * @param connectString list of servers to connect to
         * @return this
         */
        public Builder connectString(String connectString)
        {
            ensembleProvider = new FixedEnsembleProvider(connectString);
            return this;
        }

        /**
         * Set the list ensemble provider. IMPORTANT: use either this or {@link #connectString(String)}
         * but not both.
         *
         * @param ensembleProvider the ensemble provider to use
         * @return this
         */
        public Builder ensembleProvider(EnsembleProvider ensembleProvider)
        {
            this.ensembleProvider = ensembleProvider;
            return this;
        }

        /**
         * Sets the data to use when {@link PathAndBytesable#forPath(String)} is used.
         * This is useful for debugging purposes. For example, you could set this to be the IP of the
         * client.
         *
         * @param defaultData new default data to use
         * @return this
         */
        public Builder defaultData(byte[] defaultData)
        {
            this.defaultData = (defaultData != null) ? Arrays.copyOf(defaultData, defaultData.length) : null;
            return this;
        }

        /**
         * As ZooKeeper is a shared space, users of a given cluster should stay within
         * a pre-defined namespace. If a namespace is set here, all paths will get pre-pended
         * with the namespace
         *
         * @param namespace the namespace
         * @return this
         */
        public Builder namespace(String namespace)
        {
            this.namespace = namespace;
            return this;
        }

        /**
         * @param sessionTimeoutMs session timeout
         * @return this
         */
        public Builder sessionTimeoutMs(int sessionTimeoutMs)
        {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        /**
         * @param connectionTimeoutMs connection timeout
         * @return this
         */
        public Builder connectionTimeoutMs(int connectionTimeoutMs)
        {
            this.connectionTimeoutMs = connectionTimeoutMs;
            return this;
        }

        /**
         * @param retryPolicy retry policy to use
         * @return this
         */
        public Builder retryPolicy(RetryPolicy retryPolicy)
        {
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * @param threadFactory thread factory used to create Executor Services
         * @return this
         */
        public Builder threadFactory(ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * @param compressionProvider the compression provider
         * @return this
         */
        public Builder compressionProvider(CompressionProvider compressionProvider)
        {
            this.compressionProvider = compressionProvider;
            return this;
        }

        /**
         * @param zookeeperFactory the zookeeper factory to use
         * @return this
         */
        public Builder zookeeperFactory(ZookeeperFactory zookeeperFactory)
        {
            this.zookeeperFactory = zookeeperFactory;
            return this;
        }

        /**
         * @param aclProvider a provider for ACLs
         * @return this
         */
        public Builder aclProvider(ACLProvider aclProvider)
        {
            this.aclProvider = aclProvider;
            return this;
        }

        /**
         * @param canBeReadOnly if true, allow ZooKeeper client to enter
         *                      read only mode in case of a network partition. See
         *                      {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
         *                      for details
         * @return this
         */
        public Builder canBeReadOnly(boolean canBeReadOnly)
        {
            this.canBeReadOnly = canBeReadOnly;
            return this;
        }

        public ACLProvider getAclProvider()
        {
            return aclProvider;
        }

        public ZookeeperFactory getZookeeperFactory()
        {
            return zookeeperFactory;
        }

        public CompressionProvider getCompressionProvider()
        {
            return compressionProvider;
        }

        public ThreadFactory getThreadFactory()
        {
            return threadFactory;
        }

        public EnsembleProvider getEnsembleProvider()
        {
            return ensembleProvider;
        }

        public int getSessionTimeoutMs()
        {
            return sessionTimeoutMs;
        }

        public int getConnectionTimeoutMs()
        {
            return connectionTimeoutMs;
        }

        public RetryPolicy getRetryPolicy()
        {
            return retryPolicy;
        }

        public String getNamespace()
        {
            return namespace;
        }

        public String getAuthScheme()
        {
            return authScheme;
        }

        public byte[] getAuthValue()
        {
            return (authValue != null) ? Arrays.copyOf(authValue, authValue.length) : null;
        }

        public byte[] getDefaultData()
        {
            return defaultData;
        }

        public boolean canBeReadOnly()
        {
            return canBeReadOnly;
        }

        private Builder()
        {
        }
    }

    private static byte[] getLocalAddress()
    {
        try
        {
            return InetAddress.getLocalHost().getHostAddress().getBytes();
        }
        catch ( UnknownHostException ignore )
        {
            // ignore
        }
        return new byte[0];
    }

    private CuratorFrameworkFactory()
    {
    }
}
