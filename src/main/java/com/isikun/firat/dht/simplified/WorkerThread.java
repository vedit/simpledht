package com.isikun.firat.dht.simplified;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

// Thread for sending messages
public class WorkerThread implements Runnable {

    private final Socket clientSocket;

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
            response = processResponse(request);
            System.out.println("!!!!RESPONSE!!!!");
            System.out.println(response);
        } else {
            System.out.println("Request is null");
        }

        socketOut.println(DhtMessage.serialize(response));
        System.out.println(node.getNodeId() + " recieved from " + request.getFromNodeId() + " at port:" + clientSocket.getPort() + " localport:" + clientSocket.getLocalPort() + ": \n\t\"" + request + "\"");
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

    public DhtMessage processResponse(DhtMessage request) {
        DhtNode node = DhtNode.getInstance();
        DhtMessage response = null;
        switch (request.getAction()) {
            case DhtMessage.ACTION_ENTRY:
                ezLog(node.getNodeId(), request.getFromNodeId(), "ENTRY");
                System.out.println("1");
                response = node.bootstrapNode(request);
                DhtNode.getMessageQueue().offer(response);
//                node.getMessageQueue().offer(node.updateSuccessor(request));
//                System.out.println("2");
//                node.getMessageQueue().offer(node.updatePredecessor(request));
//                response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                //node.updatePredecessor(request);
                break;
            case DhtMessage.ACTION_UPDATE:
                ezLog(node.getNodeId(), request.getFromNodeId(), "UPDATE");
                response = node.processUpdateAction(request);
                DhtNode.getMessageQueue().offer(response);
//                response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                break;
            case DhtMessage.ACTION_FILE_SEND:
                ezLog(node.getNodeId(), request.getFromNodeId(), "FILE");
//                node.getMessageQueue().offer(node.processUpdateAction(request));
                if (node.processReceivedPayload(request.getPayLoad())) {
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                } else {
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ERROR, node.getPort(), request.getFromPort());
                }
                break;

            case DhtMessage.ACTION_FIND: //SUCCESSOR QUERY
                ezLog(node.getNodeId(), request.getFromNodeId(), "FIND");
                if (request.getType() == DhtMessage.TYPE_SUCCESSOR) {
                    NodeRecord successor = node.succ(Integer.parseInt(request.getPayLoad()));
                    if (successor.compareTo(node.toNodeRecord()) == 0) {
                        DhtMessage updatePred = new DhtMessage(node.getNodeId(), node.getNodeId(), DhtMessage.ACTION_UPDATE, node.getPort(), node.getPort(), DhtMessage.TYPE_PREDECESSOR, request.toNodeRecord().serialize());
                        DhtNode.getMessageQueue().offer(updatePred);
                    }
                    response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_UPDATE, node.getPort(), request.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successor.serialize());
//                    new DhtMessage(nodeId, successor.getNodeId(), DhtMessage.ACTION_ENTRY, port, referenceNodePort, DhtMessage.TYPE_SUCCESSOR, new NodeRecord(nodeId, port).serialize());
                    System.out.println("PREDECESSOR FIND?");
                }
                break;
            case DhtMessage.ACTION_BOOTSTRAPPING:
                ezLog(node.getNodeId(), request.getFromNodeId(), "BOOTSTRAP");
                System.out.println(request.getFromNodeId());
                break;
            case DhtMessage.ACTION_LEAVING:
                ezLog(node.getNodeId(), request.getFromNodeId(), "LEAVE");
                break;
            case DhtMessage.ACTION_ACK:
                ezLog(node.getNodeId(), request.getFromNodeId(), "ACK");
                response = new DhtMessage(node.getNodeId(), request.getFromNodeId(), DhtMessage.ACTION_ACK, node.getPort(), request.getFromPort());
                break;
        }
        return response;
    }

    public void ezLog(int node, int from, String action) {
        System.out.println("NODE " + node + " RECEIVED " + action + " FROM " + from);
    }

}
