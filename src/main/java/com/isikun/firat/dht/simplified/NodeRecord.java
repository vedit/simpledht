package com.isikun.firat.dht.simplified;

import java.io.Serializable;

import com.google.gson.Gson;

/**
 * Created by hexenoid on 11/23/14.
 */
public class NodeRecord implements Serializable, Comparable<NodeRecord> {
    private int nodeId;
    private int port;

    public NodeRecord() {
    }

    public NodeRecord(int nodeName, int port) {
        this.nodeId = nodeName;
        this.port = port;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String serialize() {
        Gson gson = new Gson();
        String json = gson.toJson(this);
//        System.out.println(json);
        return json;
    }

    public static NodeRecord deserialize(String noderecord) {
        Gson gson = new Gson();
//        System.out.println(message);
        return gson.fromJson(noderecord, NodeRecord.class);
    }

    @Override
    public String toString() {
        return this.serialize();
    }

    // Returns positive when first is bigger
    // Negative when second is bigger
    // 0 when they are the same
    @Override
    public int compareTo(NodeRecord nodeRecord) {
        return nodeId - nodeRecord.getNodeId();
    }

}
