package com.isikun.firat.dht.simplified;

/**
 * Created by hexenoid on 12/23/14.
 */
public class FingerTable {
    private final NodeRecord[] fingerTable;
    private final DhtNode node;

    FingerTable() {
        fingerTable = new NodeRecord[DhtNode.KEY_SPACE];
        node = DhtNode.getInstance();
        for(int i=0; i<fingerTable.length; i++){
            fingerTable[i] = node.toNodeRecord();
        }
    }

    public NodeRecord getClosestPrecedingNode(int k){
        NodeRecord result = fingerTable[fingerTable.length + 1];
        for(int i=0; i<fingerTable.length; i++){
            if(fingerTable[i].getNodeId() > k && i > 0){
                result = fingerTable[i-1];
            }
        }

        return result;
    }

    public void stabilize(){
        
    }
}
