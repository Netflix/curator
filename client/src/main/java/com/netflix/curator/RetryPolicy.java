/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator;

/**
 * Abstracts the policy to use when retrying connections
 */
public interface RetryPolicy
{
    /**
     * Called when an operation has failed for some reason. This method should return
     * true to make another attempt.
     *
     * @param retryCount the number of times retried so far (0 the first time)
     * @param elapsedTimeMs the elapsed time in ms since the operation was attempted
     * @return true/false
     */
    public boolean      allowRetry(int retryCount, long elapsedTimeMs);
}
