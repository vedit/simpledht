package com.isikun.firat.dht.simplified;

import com.google.gson.Gson;

/**
 * Created by hexenoid on 12/23/14.
 */
public class FingerTable {
    private final NodeRecord[] fingerTable;

    FingerTable() {
        fingerTable = new NodeRecord[DhtNode.KEY_SPACE];
        DhtNode node = DhtNode.getInstance();
        for (int i = 0; i < fingerTable.length; i++) {
            fingerTable[i] = node.toNodeRecord();
        }
        for(NodeRecord row: fingerTable){
            System.out.println(row);
        }
//        System.out.println(fingerTable);
    }

    public NodeRecord getClosestPrecedingNode(int k) {
        NodeRecord result = DhtNode.getInstance().toNodeRecord();
        for (int i = 0; i < fingerTable.length; i++) {
            if (fingerTable[i].getNodeId() > k && i > 0) {
                result = fingerTable[i - 1];
            }
        }
        return result;
    }

    public NodeRecord getFirst(){
        return fingerTable[0];
    }

    public synchronized void stabilize() {
        DhtNode node = DhtNode.getInstance();
        for (int i = 0; i < fingerTable.length; i++) {
            fingerTable[i] = node.succ(node.getNodeId() + (int) Math.pow(2, i)); //pow(2, i-1) olacakti 0 based olmasaydi
        }
    }

    public String toString(){
        Gson gson = new Gson();
        String json = gson.toJson(fingerTable);
        return json;
    }
}
