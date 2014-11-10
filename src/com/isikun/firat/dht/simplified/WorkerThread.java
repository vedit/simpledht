package com.isikun.firat.dht.simplified;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.ParseException;

public class WorkerThread implements Runnable{

    private Socket clientSocket;
    public WorkerThread(Socket s) {
        clientSocket = s;
    }
    public void run() {
        //taken from Server4SingleClient
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;

        try {
            //will use socketOut to send text to the server over the socket
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            //will use socketIn to receive text from the server over the socket
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e) {
            System.out.println("Cannot get I/O for the connection.");
            e.printStackTrace();
            System.exit(0);
        }

        DhtMessage request = null;
        DhtMessage response = null;
        try {
            request = DhtMessage.parseMessage(socketIn.readLine());
            System.out.println(request);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (request != null) {
            switch(request.getAction()){
                case DhtMessage.ACTION_BOOTSTRAPPING:
                    System.out.println(request.getFromNodeId());
                    response = bootstrapNode(request);
                    break;
                case DhtMessage.ACTION_FIND:
                    System.out.println(request.getFromNodeId());
                    response = findNode(request);
                    break;
                case DhtMessage.ACTION_UPDATE:
                    break;
                case DhtMessage.ACTION_LEAVING:
                    break;
            }
        }

        socketOut.println(response);
        System.out.println("Client's request was: \n\t\"" + request + "\"");
        System.out.println("Your response was: \n\t\"" + response + "\"");


        //close all streams
        socketOut.close();
        try {
            socketIn.close();
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DhtMessage bootstrapNode(DhtMessage message){
        DhtNode node = DhtNode.getInstance();
        node.updateSuccessor(message);
        return new DhtMessage();
    }

    private DhtMessage findNode(DhtMessage message){
        DhtNode node = DhtNode.getInstance();
        int successor = node.succ(message.getPayLoad());
        return new DhtMessage();
    }
}
