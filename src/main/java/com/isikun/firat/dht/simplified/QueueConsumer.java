package com.isikun.firat.dht.simplified;

import java.util.Queue;

/**
 * Created by hexenoid on 11/20/14.
 */
public class QueueConsumer implements Runnable{

    Queue<DhtMessage> queue;
    private ResponseReceivedEvent asyncResponse;

    public QueueConsumer (Queue queue){
        this.queue = queue;
    }

    @Override
    public void run() {
        while(true){
            if(queue.peek() != null){
                DhtMessage message = queue.poll();
                DhtMessage response = message.sendMessage();
                asyncResponse.responseReceivedEvent(response);
            }
            try{
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
