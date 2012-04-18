package com.netflix.curator.session;

import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.RetryLoop;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used by {@link RetryLoop#sessionSafeRetry(CuratorZookeeperClient, Callable)}. You should not have
 * any need to use this class directly.
 */
public class FailingThreadSessionState implements SessionState
{
    private final Thread            ourThread = Thread.currentThread();
    private final AtomicBoolean     sessionHasFailed = new AtomicBoolean(false);
    private final SessionState      parentSessionState;

    public FailingThreadSessionState(SessionState parentSessionState)
    {
        this.parentSessionState = parentSessionState;
    }

    @Override
    public void handleSessionFailure()
    {
        sessionHasFailed.set(true);
        parentSessionState.handleSessionFailure();
    }

    @Override
    public boolean shouldThrowSessionFailure()
    {
        return isOurThread() ? sessionHasFailed.get() : parentSessionState.shouldThrowSessionFailure();
    }

    private boolean isOurThread()
    {
        return Thread.currentThread().equals(ourThread);
    }
}
