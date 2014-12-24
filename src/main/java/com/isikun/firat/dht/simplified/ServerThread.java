package com.isikun.firat.dht.simplified;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by hexenoid on 11/19/14.
 */
// Thread for receiving and processing messages
public class ServerThread implements Runnable {

    private int port;

    private static volatile ServerThread instance = null;

    private ServerThread(int port) {
        this.port = port;
    }

    public static ServerThread getInstance() {
        if (instance == null) {
            synchronized (ServerThread.class) {
                if (instance == null) {
                    instance = new ServerThread(DhtNode.getInstance().getPort());
                    new Thread(instance).start();
                }
            }
        }
        return instance;
    }

    @Override
    public synchronized void run() {
        startServer();
    }

    public synchronized void startServer() {
        ServerSocket serverSocket = null;

        try {
            //initialize server socket
            serverSocket = new ServerSocket(port);
            System.out.println("Server socket initialized at " + port + ".\n");
        } catch (IOException e) { //if this port is busy, an IOException is fired
            System.out.println("Cannot listen on port " + port);
            e.printStackTrace();
            System.exit(0);
        }

        Socket clientSocket = null;

        try {
            while (true) { //infinite loop - terminate manually
                //wait for client connections
                System.out.println("Waiting for a client connection.");
                try {
                    clientSocket = serverSocket.accept();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                //let us see who connected
                String clientName = clientSocket.getInetAddress().getHostName();
                System.out.println(clientName + " established a connection.");
                System.out.println();
                //assign a worker thread
                Thread workerThread = new Thread(new WorkerThread(clientSocket));
                workerThread.start();
                System.out.println("Worker Thread is spawned for " + DhtNode.getInstance().getNodeId() + " at " + clientSocket.getPort());
                try {
                    workerThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Worker Thread is dead for " + DhtNode.getInstance().getNodeId() + " at " + +clientSocket.getPort());

            }
        } finally {
            //make sure that the socket is closed upon termination
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
