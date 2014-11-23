package com.isikun.firat.dht.simplified;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import com.google.gson.Gson;
import org.apache.commons.codec.binary.Base64;

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

    public static final int TYPE_SUCCESSOR = 1;
    public static final int TYPE_PREDECESSOR = 2;

    public static final int ACTION_LEAVING = -1;
    public static final int ACTION_ENTRY = 2;
    public static final int ACTION_ACK = 3;
    public static final int ACTION_FIND = 4;
    public static final int ACTION_FILE_SEND = 6;
    public static final int ACTION_BOOTSTRAPPING = 1;
    public static final int ACTION_UPDATE = 5;
    public static final int ACTION_ERROR = 666;

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
        }  catch (IOException e){ //put it back to queue
            System.out.println("Cannot Connect to " + this.getToNodeId() + ":" + this.getToPort());
            DhtNode.getQueue().offer(this); //
        }
        DhtMessage response = null;
        if (socketOut != null && socketIn != null) {
            socketOut.println(DhtMessage.serialize(this));
            try {
                response = DhtMessage.deserialize(socketIn.readLine());
//                System.out.println(response);
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

    public static String serialize(DhtMessage message){
        Gson gson = new Gson();
        String json = gson.toJson(message);
//        System.out.println(json);
        return json;
    }

    public static DhtMessage deserialize(String message){
        Gson gson = new Gson();
//        System.out.println(message);
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

}
