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