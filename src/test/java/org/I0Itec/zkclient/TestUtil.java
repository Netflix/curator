package org.I0Itec.zkclient;

import org.mockito.exceptions.base.MockitoAssertionError;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class TestUtil {

    /**
     * This waits until the provided {@link Callable} returns an object that is equals to the given expected value or
     * the timeout has been reached. In both cases this method will return the return value of the latest
     * {@link Callable} execution.
     * 
     * @param expectedValue
     *            The expected value of the callable.
     * @param callable
     *            The callable.
     * @param <T>
     *            The return type of the callable.
     * @param timeUnit
     *            The timeout timeunit.
     * @param timeout
     *            The timeout.
     * @return the return value of the latest {@link Callable} execution.
     * @throws Exception
     * @throws InterruptedException
     */
    public static <T> T waitUntil(T expectedValue, Callable<T> callable, TimeUnit timeUnit, long timeout) throws Exception {
        long startTime = System.currentTimeMillis();
        do {
            T actual = callable.call();
            if (expectedValue.equals(actual)) {
                return actual;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                return actual;
            }
            Thread.sleep(50);
        } while (true);
    }

    /**
     * This waits until a mockito verification passed (which is provided in the runnable). This waits until the
     * virification passed or the timeout has been reached. If the timeout has been reached this method will rethrow the
     * {@link MockitoAssertionError} that comes from the mockito verification code.
     * 
     * @param runnable
     *            The runnable containing the mockito verification.
     * @param timeUnit
     *            The timeout timeunit.
     * @param timeout
     *            The timeout.
     * @throws InterruptedException
     */
    public static void waitUntilVerified(Runnable runnable, TimeUnit timeUnit, int timeout) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        do {
            MockitoAssertionError exception = null;
            try {
                runnable.run();
            } catch (MockitoAssertionError e) {
                exception = e;
            }
            if (exception == null) {
                return;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                throw exception;
            }
            Thread.sleep(50);
        } while (true);
    }
}