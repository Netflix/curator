/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

public class DeferredGatewayStarter extends Thread {

    private final Gateway _zkServer;
    private int _delay;

    public DeferredGatewayStarter(Gateway gateway, int delay) {
        _zkServer = gateway;
        _delay = delay;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(_delay);
            _zkServer.start();
        } catch (Exception e) {
            // ignore
        }
    }
}