package com.isikun.firat.dht.simplified;

import java.util.concurrent.CountDownLatch;

/**
 * Created by hexenoid on 12/22/14.
 */
public class ResponseListener implements ResponseReceivedEvent, Runnable {

    private volatile DhtMessage response;
    private CountDownLatch latch;
    private boolean responseReceived = false;
    private boolean running = true;
    private String callback;

    public ResponseListener(CountDownLatch latch, String callback){
        this.callback = callback;
        this.latch = latch;
    }

    @Override
    public void responseReceivedEvent(DhtMessage message) {
        if(message.getCallback() == callback){
            response = message;
            responseReceived = true;
        }
    }

    @Override
    public void run() {
        while(true){
            if(responseReceived){
                latch.countDown();
                stopRunning();
            }
            try{
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    public void stopRunning(){
        running = false;
    }

    public DhtMessage getResponse(){
        return response;
    }
}
