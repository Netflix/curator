package com.netflix.curator.session;

import org.apache.zookeeper.KeeperException;

/**
 * Thrown when {@link SessionState#shouldThrowSessionFailure()} returns true
 */
public class SessionFailedException extends Exception
{
    /**
     * Used internally
     *
     * @return exception as a ZooKeeper exception
     */
    public KeeperException      asKeeperException()
    {
        return new KeeperException.SessionExpiredException();
    }
}
