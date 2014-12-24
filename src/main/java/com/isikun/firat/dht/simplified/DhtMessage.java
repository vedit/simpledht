package com.isikun.firat.dht.simplified;

import com.google.gson.Gson;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by hexenoid on 10/13/14.
 */
public class DhtMessage implements Serializable {
    private int fromNodeId;
    private int toNodeId;
    private int action;
    private int fromPort;
    private int toPort;
    private int type;
    private String payLoad;
    private int payloadOwnerPort;
    private String callback;

    public static final int TYPE_SUCCESSOR = 1;
    public static final int TYPE_PREDECESSOR = 2;

    public static final int ACTION_ACK = 3;
    public static final int ACTION_FIND = 4;
    public static final int ACTION_UPDATE = 5;
    public static final int ACTION_FILE_SEND = 6;
    public static final int ACTION_BOOTSTRAPPING = 1;
    public static final int ACTION_ERROR = 666;

    public static final int STATE_FOUND = 7;
    public static final int STATE_LEAVING = -1;
    public static final int STATE_ENTRY = 2;

    public DhtMessage() {

    }

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.action = action;
        this.fromPort = fromPort;
        this.toPort = toPort;
    }

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort, int type) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.action = action;
        this.type = type;
        this.fromPort = fromPort;
        this.toPort = toPort;
    }

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort, int type, String payLoad) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.action = action;
        this.type = type;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.payLoad = payLoad;
    }

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort, int type, String payLoad, int payloadOwnerPort) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.action = action;
        this.type = type;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.payLoad = payLoad;
        this.payloadOwnerPort = payloadOwnerPort;
    }

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort, int type, String payLoad, int payloadOwnerPort, String callback) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.action = action;
        this.type = type;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.payLoad = payLoad;
        this.payloadOwnerPort = payloadOwnerPort;
        this.callback = callback;
    }

    public DhtMessage sendMessage() {
        Socket clientSocket = null;
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        String host = "127.0.0.1";
//        System.out.println(request);
        try {
            //create socket and connect to the server
            clientSocket = new Socket(host, this.getToPort());
            //will use socketOut to send text to the server over the socket
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            //will use socketIn to receive text from the server over the socket
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (UnknownHostException e) { //if serverName cannot be resolved to an address
            System.out.println("Who is " + host + "?");
            e.printStackTrace();
        } catch (IOException e) { //put it back to queue
            System.out.println("Cannot Connect to " + this.getToNodeId() + ":" + this.getToPort());
            DhtNode.getMessageQueue().offer(this); //
        }
        DhtMessage response = null;
        if (socketOut != null && socketIn != null) {
            socketOut.println(DhtMessage.serialize(this));
            try {
                response = DhtMessage.deserialize(socketIn.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }
            socketOut.close();
            try {
                socketIn.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (socketIn == null) {
            System.out.println("SocketOut is null");
        }
        if (socketOut == null) {
            System.out.println("SocketIn is null");
        }
        return response;
    }

    public static DhtMessage findSuccessor(NodeRecord to, String payLoad) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), to.getNodeId(), DhtMessage.ACTION_FIND, from.getPort(), to.getPort(), DhtMessage.TYPE_SUCCESSOR, payLoad);
    }

    public static DhtMessage getSuccessorsPrecessor(NodeRecord to) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), from.getNodeId(), DhtMessage.ACTION_FIND, from.getPort(), to.getPort(), DhtMessage.TYPE_PREDECESSOR);
    }

    public static DhtMessage sendFile(NodeRecord to, String payLoad) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), to.getNodeId(), DhtMessage.ACTION_FILE_SEND, from.getPort(), to.getPort(), DhtMessage.TYPE_SUCCESSOR, payLoad);
    }

    public static DhtMessage updateSuccessor(NodeRecord to, String payLoad) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), to.getNodeId(), DhtMessage.ACTION_UPDATE, from.getPort(), to.getPort(), DhtMessage.TYPE_SUCCESSOR, payLoad);
    }

    public static DhtMessage updatePredecessor(NodeRecord to, String payLoad) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), to.getNodeId(), DhtMessage.ACTION_UPDATE, from.getPort(), to.getPort(), DhtMessage.TYPE_PREDECESSOR, payLoad);
    }

    public static DhtMessage sendAck(NodeRecord to) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), to.getNodeId(), DhtMessage.ACTION_ACK, from.getPort(), to.getPort());
    }

    public static DhtMessage sendError(NodeRecord to) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), to.getNodeId(), DhtMessage.ACTION_ERROR, from.getPort(), to.getPort());
    }

    public static DhtMessage makeEntry(int referenceNodePort) {
        NodeRecord from = DhtNode.getInstance().toNodeRecord();
        return new DhtMessage(from.getNodeId(), from.getNodeId(), DhtMessage.STATE_ENTRY, from.getPort(), referenceNodePort, DhtMessage.TYPE_SUCCESSOR, from.serialize());
    }

    public static DhtMessage processResponse(DhtMessage request) {
        DhtNode node = DhtNode.getInstance();
        DhtMessage response = null;
        node.checkIfNeighbor(request);
        switch (request.getAction()) {
            case DhtMessage.STATE_ENTRY:
                ezLog(node.getNodeId(), request.getFromNodeId(), "ENTRY");
                System.out.println("1");
                response = node.bootstrapNode(request);
                DhtNode.getMessageQueue().offer(response);
                break;
            case DhtMessage.ACTION_UPDATE:
                ezLog(node.getNodeId(), request.getFromNodeId(), "UPDATE");
                response = node.processUpdateAction(request);
                DhtNode.getMessageQueue().offer(response);
                break;
            case DhtMessage.ACTION_FILE_SEND:
                ezLog(node.getNodeId(), request.getFromNodeId(), "FILE");
                if (node.processReceivedPayload(request.getPayLoad())) {
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                } else {
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ERROR, node.getPort(), request.getFromPort());
                }
                break;
            case DhtMessage.ACTION_FIND: //SUCCESSOR QUERY
                ezLog(node.getNodeId(), request.getFromNodeId(), "FIND");
                if (request.getType() == DhtMessage.TYPE_SUCCESSOR) {
                    System.out.println("SUCCESSOR FIND?");
                    NodeRecord successor = node.succ(Integer.parseInt(request.getPayLoad()));
                    if (successor.compareTo(node.toNodeRecord()) == 0) {
                        DhtMessage updatePred = new DhtMessage(node.getNodeId(), node.getNodeId(), DhtMessage.ACTION_UPDATE, node.getPort(), node.getPort(), DhtMessage.TYPE_PREDECESSOR, request.toNodeRecord().serialize());
                        DhtNode.getMessageQueue().offer(updatePred);
                    }
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.STATE_FOUND, node.getPort(), request.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successor.serialize());
                } else if (request.getType() == DhtMessage.TYPE_PREDECESSOR) {
                    System.out.println("PREDECESSOR FIND?");
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.STATE_FOUND, node.getPort(), request.getFromPort(), DhtMessage.TYPE_PREDECESSOR, node.getPredecessor().serialize());
                }
                break;
            case DhtMessage.STATE_FOUND:
                response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                break;
            case DhtMessage.ACTION_BOOTSTRAPPING:
                ezLog(node.getNodeId(), request.getFromNodeId(), "BOOTSTRAP");
                System.out.println(request.getFromNodeId());
                break;
            case DhtMessage.STATE_LEAVING:
                ezLog(node.getNodeId(), request.getFromNodeId(), "LEAVE");
                break;
            case DhtMessage.ACTION_ACK:
                ezLog(node.getNodeId(), request.getFromNodeId(), "ACK");
                response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                break;
        }
        return response;
    }

    public static void ezLog(int node, int from, String action) {
        System.out.println("NODE " + node + " RECEIVED " + action + " FROM " + from);
    }

    public static String serialize(DhtMessage message) {
        Gson gson = new Gson();
        String json = gson.toJson(message);
        return json;
    }

    public static DhtMessage deserialize(String message) {
        Gson gson = new Gson();
        return gson.fromJson(message, DhtMessage.class);
    }

    public String toString() {
        return serialize(this);
    }

    public int getFromNodeId() {
        return fromNodeId;
    }

    public void setFromNodeId(int fromNodeId) {
        this.fromNodeId = fromNodeId;
    }

    public int getFromPort() {
        return fromPort;
    }

    public void setFromPort(int fromPort) {
        this.fromPort = fromPort;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public int getToNodeId() {
        return toNodeId;
    }

    public void setToNodeId(int toNodeId) {
        this.toNodeId = toNodeId;
    }

    public String getPayLoad() {
        return payLoad;
    }

    public void setPayLoad(String payLoad) {
        this.payLoad = payLoad;
    }

    public int getToPort() {
        return toPort;
    }

    public void setToPort(int toPort) {
        this.toPort = toPort;
    }

    public int getPayloadOwnerPort() {
        return payloadOwnerPort;
    }

    public void setPayloadOwnerPort(int payloadOwnerPort) {
        this.payloadOwnerPort = payloadOwnerPort;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public NodeRecord toNodeRecord() {
        return new NodeRecord(this.getFromNodeId(), this.getFromPort());
    }

    public NodeRecord payloadToNodeRecord() {
        return NodeRecord.deserialize(this.getPayLoad());
    }
}
