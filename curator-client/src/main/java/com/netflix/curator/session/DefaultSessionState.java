package com.netflix.curator.session;

/**
 * The default session state handler. A NOP handler.
 */
public class DefaultSessionState implements SessionState
{
    @Override
    public void handleSessionFailure()
    {
        // NOP
    }

    @Override
    public boolean shouldThrowSessionFailure()
    {
        return false;
    }
}
