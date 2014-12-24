package com.isikun.firat.dht.simplified;

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
        System.out.println(this);
    }

    public NodeRecord getClosestPrecedingNode(int k) {
        NodeRecord result = DhtNode.getInstance().toNodeRecord();
        for (int i = 1; i < fingerTable.length; i++) {
            if (Utils.isInInterval(k, fingerTable[i - 1].getNodeId(), fingerTable[i].getNodeId()) && i > 0) {
                result = fingerTable[i - 1];
            }
        }
        return result;
    }

    public NodeRecord getFirst() {
        return fingerTable[0];
    }

    public synchronized void stabilize() {
        DhtNode node = DhtNode.getInstance();
        for (int i = 0; i < fingerTable.length; i++) {
            System.out.println("Calculating finger table for:" + ((node.getNodeId() + (int) Math.pow(2, i))% DhtNode.MAX_NODES));
            this.insert(node.succ(((node.getNodeId() + (int) Math.pow(2, i))% DhtNode.MAX_NODES)), i); //pow(2, i-1) olacakti 0 based olmasaydi
        }
    }

    public synchronized boolean insert(NodeRecord record, int index) {
        boolean result = false;
//        fingerTable[index] = record;
        for (int i = 1; i < fingerTable.length; i++) {
            if (Utils.isInInterval(record.getNodeId(), fingerTable[i - 1].getNodeId(), fingerTable[i].getNodeId()) && i > 0) {
                fingerTable[i - 1] = record;
                System.out.println("=0=0=0=0=0=0=0=0=::INSERTED::=> " + record);
                result = true;
            }
        }
        return result;
    }

    public boolean Contains(NodeRecord record) {
        boolean result = false;
        for (NodeRecord row : fingerTable) {
            if (row.compareTo(record) == 0) {
                result = true;
            }
        }
        return result;
    }

    public String toString() {
        String stringRepresentation = "FingerTable:\n[";
        for (NodeRecord row : fingerTable) {
            stringRepresentation += row.serialize() + "\n";
        }
        stringRepresentation += "]";
        return stringRepresentation;
    }
}
