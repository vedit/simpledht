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

    public void run() {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (request != null) {
            response = DhtMessage.processResponse(request);
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

}
