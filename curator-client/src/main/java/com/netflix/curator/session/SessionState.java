package com.netflix.curator.session;

/**
 * handling policy for session states
 */
public interface SessionState
{
    /**
     * Called when a ZK session failure has been detected
     */
    public void         handleSessionFailure();

    /**
     * Return true if the state is such that {@link SessionFailedException} should be thrown
     *
     * @return true/false
     */
    public boolean      shouldThrowSessionFailure();
}
