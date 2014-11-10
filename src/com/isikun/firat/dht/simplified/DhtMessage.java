package com.isikun.firat.dht.simplified;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;

/**
 * Created by hexenoid on 10/13/14.
 */
public class DhtMessage{
    private int fromNodeId;
    private int toNodeId;
    private int action;
    private int fromPort;
    private int toPort;
    private int type;
    private int payLoad;
    private int payloadOwnerPort;

    public static final int TYPE_SUCCESSOR = 1;
    public static final int TYPE_PREDECESSOR = 2;
    
    public static final int ACTION_LEAVING = -1;
    public static final int ACTION_BOOTSTRAPPING = 1;
    public static final int ACTION_ENTRY = 2;
    public static final int ACTION_INSYNC = 3;
    public static final int ACTION_FIND = 4;
    public static final int ACTION_UPDATE = 5;

    public DhtMessage(){

    }

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort){
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

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort, int type, int payLoad) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.action = action;
        this.type = type;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.payLoad = payLoad;
    }

    public DhtMessage(int fromNodeId, int toNodeId, int action, int fromPort, int toPort, int type, int payLoad, int payloadOwnerPort) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.action = action;
        this.type = type;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.payLoad = payLoad;
        this.payloadOwnerPort = payloadOwnerPort;
    }

    public static DhtMessage parseMessage(String message) throws ParseException {
        String[] parts = message.split(",");
        DhtMessage parsedMessage = new DhtMessage();
        for(String part: parts){
            String[] value = part.split(":");
            if (value[0].equals("fromNodeId")) {
                parsedMessage.setFromNodeId(Integer.parseInt(value[1]));
            } else if (value[0].equals("toNodeId")) {
                parsedMessage.setToNodeId(Integer.parseInt(value[1]));
            }  else if (value[0].equals("action")) {
                parsedMessage.setAction(Integer.parseInt(value[1]));
            } else if (value[0].equals("fromPort")) {
                parsedMessage.setFromPort(Integer.parseInt(value[1]));
            } else if (value[0].equals("toPort")) {
                parsedMessage.setToPort(Integer.parseInt(value[1]));
            } else if (value[0].equals("type")) {
                parsedMessage.setType(Integer.parseInt(value[1]));
            } else if (value[0].equals("payLoad")) {
                parsedMessage.setPayLoad(Integer.parseInt(value[1]));
            } else if (value[0].equals("payLoadOwnerPort")) {
                parsedMessage.setPayloadOwnerPort(Integer.parseInt(value[1]));
            } else if (value[0].equals("default")) {
                throw new ParseException("Problem parsing property name: " + value[0], 0);
            }
        }
        return parsedMessage;
    }

    public static DhtMessage sendMessage(DhtMessage request){
        Socket clientSocket = null;
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        String host = "127.0.0.1";
        try {
            //create socket and connect to the server
            clientSocket = new Socket(host, request.getToPort());
            //will use socketOut to send text to the server over the socket
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            //will use socketIn to receive text from the server over the socket
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch(UnknownHostException e) { //if serverName cannot be resolved to an address
            System.out.println("Who is " + host + "?");
            e.printStackTrace();
            System.exit(0);
        } catch (IOException e) {
            System.out.println("Cannot get I/O for the connection.");
            e.printStackTrace();
            System.exit(0);
        }
        socketOut.println(request.toString());
        DhtMessage response = null;
        try {
            response = DhtMessage.parseMessage(socketIn.readLine());
            System.out.println(response);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        socketOut.close();
        try {
            socketIn.close();
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public String toString(){
        return "fromNodeId:"+fromNodeId + ",toNodeId:"+toNodeId + ",action:"+action + ",fromPort:"+fromPort + ",toPort:"+toPort + ",type:"+type + ",payLoad:"+ payLoad + ",payloadOwnerPort:"+payloadOwnerPort;
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

    public int getPayLoad() {
        return payLoad;
    }

    public void setPayLoad(int payLoad) {
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
