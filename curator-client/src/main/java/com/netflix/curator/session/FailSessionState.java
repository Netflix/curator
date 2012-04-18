package com.netflix.curator.session;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The default session state handler treats session failure as a kind of disconnection and merely
 * tries to reconnect. This handler treats session failure as catastrophic and makes the instance
 * become a zombie.
 */
public class FailSessionState implements SessionState
{
    private final AtomicBoolean     sessionHasFailed = new AtomicBoolean(false);

    /**
     * Reset the session failed state so that a reconnection will occur
     */
    public void reset()
    {
        sessionHasFailed.set(false);
    }

    @Override
    public void handleSessionFailure()
    {
        sessionHasFailed.set(true);
    }

    @Override
    public boolean shouldThrowSessionFailure()
    {
        return sessionHasFailed.get();
    }
}
