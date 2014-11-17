package com.isikun.firat.dht.simplified;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

public class DhtNode {

    public static final String HASH_ALGORITHM = "SHA-1";
    public static final int MAX_NODES = 32;
    public static final int MAX_ID = MAX_NODES - 1;
    private final int STREAM_BUFFER_SIZE = 8192;
    private Hashtable<Integer, String> DHTFragment;

    private String nodeName;
    private int port;
    private int referenceNodePort;
    private boolean isSetup;
    private boolean isFirst;

    private static String propFileName = System.getProperty("configFile", "config.properties");

    private int nodeId;
    private int predecessorId;
    private int predecessorPort;
    private int successorId;
    private int successorPort;

    //Singleton pattern
    private static final DhtNode INSTANCE = new DhtNode();

    public static DhtNode getInstance() {
        return INSTANCE;
    }

    private DhtNode() {
        DHTFragment = new Hashtable<Integer, String>();
        loadConfig();
        nodeId = Utils.getChecksum(nodeName, false);
        predecessorId = nodeId;
        successorId = nodeId;
        startServer();
        System.out.println(nodeId);
        enterRing();
    }

    public int succ(int k) {
        int result;
        if(k == nodeId){
            result = nodeId;
        } else if (isInInterval(k, predecessorId, successorId)) {
            result = successorId;
        } else {
            //TODO BURDASIN
            DhtMessage successorQuery = new DhtMessage(nodeId, successorId, DhtMessage.ACTION_FIND, port, successorPort, DhtMessage.TYPE_SUCCESSOR, k);
            DhtMessage response = DhtMessage.sendMessage(successorQuery);
            result = response.getPayLoad();
        }
        return result;
    }

    private int lookup(int k) {
        return 0;
    }

    private int insert(String itemName){
        return 0;
    }

    private int enterRing(){

        return 0;
    }

    private int leaveRing(){

        return 0;
    }

    private int stabilize(){

        return 0;
    }

    public int updateSuccessor(DhtMessage message){
        DhtMessage response = null;
        if(this.successorId == -1){
            this.successorId = message.getFromNodeId();
            this.successorPort = message.getFromPort();
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, nodeId);
        } else if((message.getFromNodeId() < this.successorId) && (message.getFromNodeId() > this.nodeId)) {
            this.successorId = message.getFromNodeId();
            this.successorPort = message.getFromPort();
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successorId);
        } else {
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, succ(message.getFromNodeId() + 1));

        }
        return 0;
    }

    private void insertToDHT(String itemName) {
        int itemId = Utils.getChecksum(itemName, true);
        System.out.println("itemName: " + itemName + " - itemId: " + itemId);
        DHTFragment.put(itemId, itemName);
    }

    private void removeFromDHT(int itemId) {
        DHTFragment.remove(itemId);
    }


    public boolean notifyEntry(){
        Socket clientSocket = null;
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        String host = "127.0.0.1";
        try {
            //create socket and connect to the server
            clientSocket = new Socket(host, Utils.randomPort());
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
        DhtMessage request = new DhtMessage(nodeId, nodeId, DhtMessage.ACTION_BOOTSTRAPPING, port, referenceNodePort);
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
        return false;
    }

    public boolean getItem(String itemName, int tempPort) {
        Socket socket = null;
        String host = "127.0.0.1";
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        BufferedOutputStream out = null;
        boolean result = true;

        try {
            socket = new Socket(host, tempPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        File file = new File(itemName);
        byte[] buffer = new byte[STREAM_BUFFER_SIZE];
        try {
            fis = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (fis != null) {
            bis = new BufferedInputStream(fis);
        }
        if (socket != null) {
            try {
                out = new BufferedOutputStream(socket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int count;

        if (bis != null && out != null) {
            try {
                while ((count = bis.read(buffer)) > 0) {
                    try {
                        out.write(buffer, 0, count);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                bis.close();
                out.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        insertToDHT(itemName);
        } else {
            result = false;
        }

        return result;
    }

    public boolean sendItem(int k) {
        return false;
    }

    public boolean isInInterval(int key, int fromId, int toId) {
        boolean result;
        // both interval bounds are equal -> calculate out of equals
        if (fromId == toId){
            result = false;
        }

        // interval does not cross zero -> compare with both bounds
        else if (fromId < toId) {
            result = ((key > fromId) && (key < toId));
        }

        // interval crosses zero -> split interval at zero
        else {
            boolean lowerInterval = ((key > fromId) && (key <= MAX_ID));
            boolean lowerTerminationCondition = (fromId != MAX_ID);
            boolean upperInterval = ((key >= 0) && (key < toId));
            boolean upperTerminationCondition = (0 != toId);

            result = ((lowerInterval && lowerTerminationCondition) || (upperInterval && upperTerminationCondition));
        }

        return result;
    }

    public void startServer() {
        ServerSocket serverSocket = null;

        try {
            //initialize server socket
            serverSocket = new ServerSocket(port);
            System.out.println("Server socket initialized.\n");
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
                    System.exit(0);
                }

                //let us see who connected
                String clientName = clientSocket.getInetAddress().getHostName();
                System.out.println(clientName + " established a connection.");
                System.out.println();

                //assign a worker thread
                WorkerThread w = new WorkerThread(clientSocket);
                new Thread(w).start();
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

    public String getPropFileName() {
        return propFileName;
    }

    public static void setPropFileName(String prop_file_name) {
        DhtNode.propFileName = prop_file_name;
    }

    private void loadConfig() {
        Properties prop = new Properties();
        String propFileName = getPropFileName();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(propFileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            nodeName = Utils.getSafeProperty(prop, "nodeName");
            port = Integer.parseInt(Utils.getSafeProperty(prop, "fromPort"));
            referenceNodePort = Integer.parseInt(Utils.getSafeProperty(prop, "referenceNodePort"));
            isSetup = Boolean.parseBoolean(Utils.getSafeProperty(prop, "isSetup"));
            if (isSetup) {
                isFirst = Boolean.parseBoolean(Utils.getSafeProperty(prop, "isFirst"));
                if (isFirst) {
                    String[] items = Utils.getSafeProperty(prop, "items").split(",");
                    for (String item : items) {
                        insertToDHT(item);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("nodeName: " + nodeName + " - port: " + port + " - referenceNodePort: " + referenceNodePort + " - isSetup:" + isSetup + " - isFirst:" + isFirst);
    }
}
