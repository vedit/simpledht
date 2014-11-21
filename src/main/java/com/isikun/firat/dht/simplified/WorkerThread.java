package com.isikun.firat.dht.simplified;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.ParseException;
// Thread for sending messages
public class WorkerThread implements Runnable {

    private Socket clientSocket;

    public WorkerThread(Socket s) {
        clientSocket = s;
    }

    public synchronized void run() {
        //taken from Server4SingleClient
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        DhtNode node = DhtNode.getInstance();

        try {
            //will use socketOut to send text to the server over the socket
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            //will use socketIn to receive text from the server over the socket
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e) {
            System.out.println("Cannot get I/O for the connection.");
            e.printStackTrace();
            System.exit(1);
        }

        DhtMessage request = null;
        DhtMessage response = null;
        try {
            request = DhtMessage.deserialize(socketIn.readLine());
//            System.out.println(request);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (request != null) {
            switch (request.getAction()) {
                case DhtMessage.ACTION_ENTRY:
                    ezLog(node.getNodeId(),request.getFromNodeId(), "ENTRY");
                    DhtNode.getQueue().offer(node.updateSuccessor(request));
                    DhtNode.getQueue().offer(node.updatePredecessor(request));
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                    //node.updatePredecessor(request);
                    break;
                case DhtMessage.ACTION_UPDATE:
                    ezLog(node.getNodeId(), request.getFromNodeId(), "UPDATE");
                    DhtNode.getQueue().offer(node.processUpdate(request));
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                    break;
                case DhtMessage.ACTION_BOOTSTRAPPING:
                    ezLog(node.getNodeId(),request.getFromNodeId(), "BOOTSTRAP");
                    System.out.println(request.getFromNodeId());
                    response = bootstrapNode(request);
                    break;
                case DhtMessage.ACTION_FIND:
                    ezLog(node.getNodeId(), request.getFromNodeId(), "FIND");
//                    response = findNode(request);
                    break;
                case DhtMessage.ACTION_LEAVING:
                    ezLog(node.getNodeId(),request.getFromNodeId(), "LEAVE");
                    break;
                case DhtMessage.ACTION_ACK:
                    ezLog(node.getNodeId(), request.getFromNodeId(), "ACK");
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                    break;
            }
        } else {
            System.out.println("Request is null");
        }

        socketOut.println(DhtMessage.serialize(response));
        System.out.println(node.getNodeId() + " recieved from "+ request.getFromNodeId() +" at port:"+ clientSocket.getPort() + " localport:" + clientSocket.getLocalPort() +": \n\t\"" + request + "\"");
        System.out.println(node.getNodeId() + " sent: \n\t\"" + response + "\"");


        //close all streams
        socketOut.close();
        try {
            socketIn.close();
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ezLog(int node, int from, String action){
        System.out.println("NODE " + node + " RECEIVED "+ action+" FROM " + from);
    }
    private DhtMessage bootstrapNode(DhtMessage message) {
        DhtNode node = DhtNode.getInstance();
        node.updateSuccessor(message);
        return new DhtMessage();
    }

    private DhtMessage findNode(DhtMessage message) {
        DhtNode node = DhtNode.getInstance();
        int successor = node.succ(message.getPayLoad());
        return new DhtMessage();
    }
}
