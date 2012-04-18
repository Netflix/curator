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
package com.netflix.curator;

import com.google.common.base.Preconditions;
import com.netflix.curator.drivers.TracerDriver;
import com.netflix.curator.session.FailingThreadSessionState;
import com.netflix.curator.session.SessionFailedException;
import com.netflix.curator.session.SessionFailedHandler;
import com.netflix.curator.session.SessionState;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>Mechanism to perform an operation on Zookeeper that is safe against
 * disconnections and "recoverable" errors.</p>
 *
 * <p>
 * If an exception occurs during the operation, the RetryLoop will process it,
 * check with the current retry policy and either attempt to reconnect or re-throw
 * the exception
 * </p>
 *
 * Canonical usage:<br/>
 * <code><pre>
 * RetryLoop retryLoop = client.newRetryLoop();
 * while ( retryLoop.shouldContinue() )
 * {
 *     try
 *     {
 *         // do your work
 *         ZooKeeper      zk = client.getZooKeeper();    // it's important to re-get the ZK instance in case there was an error and the instance was re-created
 *
 *         retryLoop.markComplete();
 *     }
 *     catch ( Exception e )
 *     {
 *         retryLoop.takeException(e);
 *     }
 * }
 * </pre></code>
 */
public class RetryLoop
{
    private boolean         isDone = false;
    private int             retryCount = 0;

    private final Logger                        log = LoggerFactory.getLogger(getClass());
    private final long                          startTimeMs = System.currentTimeMillis();
    private final RetryPolicy                   retryPolicy;
    private final AtomicReference<TracerDriver> tracer;

    private static final SessionFailedHandler DEFAULT_SESSION_FAILED_HANDLER = new SessionFailedHandler()
    {
        @Override
        public boolean shouldAttemptToRetry()
        {
            return true;
        }
    };

    /**
     * <p>
     *     The standard retry loop does not handle session failure in any particular way. For most
     *     cases this is fine. However, there are cases where you are performing multiple operations
     *     that all must be part of the same ZooKeeper session. If at any point the ZK session fails,
     *     all further operations must fail.
     * </p>
     *
     * <p>
     *     The standard retry loop does NOT do this. Here's a scenario:
     *     <ul>
     *         <li>In a retry loop, you create an ephemeral node</li>
     *         <li>This ephemeral node is a lock/marker that MUST exist for the remainder of the operation</li>
     *         <li>In another retry loop you create some other node</li>
     *         <ul>
     *             <li>If the ZK session is lost during the create, the ephemeral node will get deleted
     *             by ZooKeeper</li>
     *             <li>However, because you are in a retry loop, the create will eventually succeed.</li>
     *         </ul>
     *     </ul>
     * </p>
     *
     * <p>
     *     For scenarios such as this use sessionSafeRetry(). Curator methods called by the callable
     *     passed to sessionSafeRetry() will fail if the ZK session fails. In other words, once
     *     a session failure is detected, future Curator calls will throw {@link SessionFailedException}
     *     (even if called from inside a standard Retry Loop).
     * </p>
     *
     * <p>
     *     <b>NOTE: </b> this guarantee is only for Curator methods called in the same {@link Thread} that
     *     made the initial call to sessionSafeRetry().
     * </p>
     *
     * @param client Zookeeper
     * @param proc procedure to call with retry
     * @param <T> return type
     * @return procedure result
     * @throws Exception any non-retriable errors
     */
    public static<T> T      sessionSafeRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception
    {
        return sessionSafeRetry(client, DEFAULT_SESSION_FAILED_HANDLER, proc);
    }

    /**
     * See {@link #sessionSafeRetry(CuratorZookeeperClient, Callable)} for details. The only difference
     * is that this version takes a {@link SessionFailedHandler} functor that controls how to handle
     * session failures.
     *
     * @param client Zookeeper
     * @param sessionFailedHandler the functor
     * @param proc procedure to call with retry
     * @param <T> return type
     * @return procedure result
     * @throws Exception any non-retriable errors
     */
    public static<T> T      sessionSafeRetry(CuratorZookeeperClient client, SessionFailedHandler sessionFailedHandler, Callable<T> proc) throws Exception
    {
        sessionFailedHandler = Preconditions.checkNotNull(sessionFailedHandler, "SessionFailedHandler cannot be null");

        T               result = null;
        RetryLoop       retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            SessionState    previousSessionState = client.getSessionState();
            try
            {
                client.setSessionState(new FailingThreadSessionState(previousSessionState));
                client.internalBlockUntilConnectedOrTimedOut();

                result = proc.call();
                retryLoop.markComplete();
            }
            catch ( SessionFailedException e )
            {
                if ( sessionFailedHandler.shouldAttemptToRetry() )
                {
                    retryLoop.takeException(e.asKeeperException());
                }
                else
                {
                    throw e;
                }
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
            finally
            {
                client.setSessionState(previousSessionState);
            }
        }
        return result;
    }

    /**
     * Convenience utility: creates a retry loop calling the given proc and retrying if needed
     *
     * @param client Zookeeper
     * @param proc procedure to call with retry
     * @param <T> return type
     * @return procedure result
     * @throws Exception any non-retriable errors
     */
    public static<T> T      callWithRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception
    {
        T               result = null;
        RetryLoop       retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                client.internalBlockUntilConnectedOrTimedOut();
                
                result = proc.call();
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
        }
        return result;
    }

    RetryLoop(RetryPolicy retryPolicy, AtomicReference<TracerDriver> tracer)
    {
        this.retryPolicy = retryPolicy;
        this.tracer = tracer;
    }

    /**
     * If true is returned, make an attempt at the operation
     *
     * @return true/false
     */
    public boolean      shouldContinue()
    {
        return !isDone;
    }

    /**
     * Call this when your operation has successfully completed
     */
    public void     markComplete()
    {
        isDone = true;
    }

    /**
     * Utility - return true if the given Zookeeper result code is retry-able
     *
     * @param rc result code
     * @return true/false
     */
    public static boolean      shouldRetry(int rc)
    {
        return (rc == KeeperException.Code.CONNECTIONLOSS.intValue()) ||
            (rc == KeeperException.Code.OPERATIONTIMEOUT.intValue()) ||
            (rc == KeeperException.Code.SESSIONMOVED.intValue()) ||
            (rc == KeeperException.Code.SESSIONEXPIRED.intValue());
    }

    /**
     * Utility - return true if the given exception is retry-able
     *
     * @param exception exception to check
     * @return true/false
     */
    public static boolean      isRetryException(Throwable exception)
    {
        if ( exception instanceof KeeperException )
        {
            KeeperException     keeperException = (KeeperException)exception;
            return shouldRetry(keeperException.code().intValue());
        }
        return false;
    }

    /**
     * Pass any caught exceptions here
     *
     * @param exception the exception
     * @throws Exception if not retry-able or the retry policy returned negative
     */
    public void         takeException(Exception exception) throws Exception
    {
        boolean     rethrow = true;
        if ( isRetryException(exception) )
        {
            log.debug("Retry-able exception received", exception);
            if ( retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startTimeMs) )
            {
                tracer.get().addCount("retries-disallowed", 1);
                log.debug("Retry policy not allowing retry");
                rethrow = false;
            }
            else
            {
                tracer.get().addCount("retries-allowed", 1);
                log.debug("Retrying operation");
            }
        }

        if ( rethrow )
        {
            throw exception;
        }
    }
}
