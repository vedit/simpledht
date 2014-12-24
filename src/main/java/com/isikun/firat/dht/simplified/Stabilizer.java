package com.isikun.firat.dht.simplified;

/**
 * Created by hexenoid on 12/24/14.
 */
public class Stabilizer implements Runnable {

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stabilize();
        }
    }

    public void stabilize() {
        System.out.println(DhtNode.getFingerTable());
        DhtMessage query = DhtMessage.getSuccessorsPrecessor(DhtNode.getFingerTable().getFirst());
        NodeRecord successorsPrecessor = query.sendMessage().toNodeRecord();
        if (DhtNode.getInstance().toNodeRecord().compareTo(successorsPrecessor) != 0) {
            System.out.println("Stabilizing Finger Table");
            DhtNode.getFingerTable().stabilize();
        } else {
            System.out.println("NO stabilization needed");
        }
    }
}
