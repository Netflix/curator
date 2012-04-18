package com.netflix.curator.session;

import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.RetryLoop;
import java.util.concurrent.Callable;

/**
 * Functor to direct {@link RetryLoop#sessionSafeRetry(CuratorZookeeperClient, SessionFailedHandler, Callable)} how to
 * handle a session failure
 */
public interface SessionFailedHandler
{
    /**
     * Called when {@link SessionFailedException} is caught. Return true to have the retry policy
     * checked and the operation potentially retried. Return false to have the exception re-thrown.
     *
     * @return true/false
     */
    public boolean      shouldAttemptToRetry();
}
