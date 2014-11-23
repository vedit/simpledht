package com.isikun.firat.dht.simplified;

import com.google.gson.Gson;
import sun.security.pkcs.EncodingException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    private String folder;

    private static String propFileName = System.getProperty("configFile", "config1.properties");

    private int nodeId;
    private int predecessorId;
    private int predecessorPort;
    private int successorId;
    private int successorPort;

    private static Queue<DhtMessage> queue;
    private static QueueConsumer consumer;

    private ServerThread socketServer;
    //Singleton pattern
    private static DhtNode INSTANCE;

    public static DhtNode getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DhtNode();
        }
        return INSTANCE;
    }

    private DhtNode() {
        DHTFragment = new Hashtable<Integer, String>();
        queue = new ConcurrentLinkedQueue<DhtMessage>();
        loadConfig();
        nodeId = Utils.getChecksum(nodeName, false);
        predecessorId = nodeId;
        successorId = nodeId;
        socketServer = new ServerThread(port);
        new Thread(socketServer).start();
        consumer = new QueueConsumer(queue);
        new Thread(consumer).start();
        enterRing();
    }

    public int succ(int k) {
        int result;
        if (k == nodeId) {
            result = nodeId;
        } else if (isInInterval(k, predecessorId, successorId)) {
            result = successorId;
        } else {
            //TODO BURDASIN
            DhtMessage successorQuery = new DhtMessage(nodeId, successorId, DhtMessage.ACTION_FIND, port, successorPort, DhtMessage.TYPE_SUCCESSOR, k + "");
            DhtMessage response = successorQuery.sendMessage();
            result = Integer.parseInt(response.getPayLoad());
        }
        return result;
    }

    private int lookup(int k) {
        return 0;
    }

    private int insert(String itemName) {
        return 0;
    }

    private boolean enterRing() {
        boolean result = false;
        DhtMessage successorQuery = new DhtMessage(nodeId, successorId, DhtMessage.ACTION_ENTRY, port, referenceNodePort);
        DhtMessage response = successorQuery.sendMessage();
//        result = response.getPayLoad();
//        successorPort = result;
        return result;
    }

    private int leaveRing() {

        return 0;
    }

    private int stabilize() {

        return 0;
    }

    public DhtMessage updateSuccessor(DhtMessage message) {
        DhtMessage response;
        if (this.successorId == this.nodeId) { // if successor is not set optimization
            this.successorId = message.getFromNodeId();
            this.successorPort = message.getFromPort();
            // let our successor know we are her predecessor
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_PREDECESSOR, nodeId + "");
        } else if ((message.getFromNodeId() < this.successorId) && (message.getFromNodeId() > this.nodeId)) { //new successor without crossing 0
            this.successorId = Integer.parseInt(message.getPayLoad());
            this.successorPort = message.getPayloadOwnerPort();
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successorId + "");
        } else { //Find the successor
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, succ(message.getFromNodeId() + 1) + "");
        }
        for(int key: getFileKeys()){
            if(isInInterval(key, nodeId, this.successorId)){
                sendItem(key);
            }
        }
        return response;
    }

    public DhtMessage updatePredecessor(DhtMessage message) {
        // TODO same as updatesuccessor currently
        DhtMessage response;
        if (this.predecessorId == this.nodeId) { // if successor is not set optimization
            this.predecessorId = message.getFromNodeId();
            this.predecessorPort = message.getFromPort();
            // let our successor know we are her successor
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, nodeId + "");
        } else if ((message.getFromNodeId() < this.successorId) && (message.getFromNodeId() > this.nodeId)) { //new successor without crossing 0
            this.predecessorId = message.getFromNodeId();
            this.predecessorPort = message.getFromPort();
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successorId + "");
        } else { //Find the successor
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, succ(message.getFromNodeId() + 1) + "");

        }
        return response;
    }

    public DhtMessage processUpdateAction(DhtMessage request) {
        DhtMessage response = null;
        if (request.getAction() == DhtMessage.ACTION_UPDATE) {
            switch (request.getType()) {
                case DhtMessage.TYPE_PREDECESSOR:
                    this.predecessorId = Integer.parseInt(request.getPayLoad());
                    System.out.println("Node " + nodeId + " predecessor is now " + this.predecessorId);
                    response = new DhtMessage(nodeId, request.getFromNodeId(), DhtMessage.ACTION_ACK, port, request.getFromPort());
                    break;
                case DhtMessage.TYPE_SUCCESSOR:
                    this.successorId = Integer.parseInt(request.getPayLoad());
                    System.out.println("Node " + nodeId + " successor is now " + this.successorId);
                    response = new DhtMessage(nodeId, request.getFromNodeId(), DhtMessage.ACTION_ACK, port, request.getFromPort());
                    break;
                default:
                    response = new DhtMessage(nodeId, request.getFromNodeId(), DhtMessage.ACTION_ERROR, port, request.getFromPort());
                    break;
            }

        }
        return response;
    }

    private void insertToDHT(String itemName) {
        int itemId = Utils.getChecksum(folder + "/" + itemName, true);
//        System.out.println("itemName: " + itemName + " - itemId: " + itemId);
        DHTFragment.put(itemId, itemName);
    }

    private void removeFromDHT(int itemId) {
        if(deleteFile(DHTFragment.get(itemId))){
            DHTFragment.remove(itemId);
        }
    }

    public String processSentPayload(int key){
        Gson gson = new Gson();
        String fileName = DHTFragment.get(key);
        String fileContents = Utils.encodePayload(readFile(fileName));
        String[] payload = {fileName, fileContents};
        String processedPayload = gson.toJson(payload);
        return processedPayload;
    }

    public boolean processReceivedPayload(String processedPayload){
        boolean result = false;
        Gson gson = new Gson();
        String[] payload = gson.fromJson(processedPayload, String[].class);
        if (saveFile(payload)){
            insertToDHT(payload[0]);
            System.out.println(payload[0] + " : " + payload[1] + " is saved");
            result = true;
        }
        return result;
    }

    public String readFile(String fileName){
        String fileContents = null;
        try(BufferedReader br = new BufferedReader(new FileReader(folder + "/" + fileName))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            fileContents = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileContents;
    }

    public boolean saveFile(String[] payload){
        boolean result = false;
        try {
            PrintWriter writer = new PrintWriter(folder + "/" + payload[0], "UTF-8");
            writer.println(Utils.decodePayload(payload[1]));
            writer.close();
            result = true;
        } catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean deleteFile(String fileName){
        boolean result = false;
        try{
            File file = new File(folder + "/" + fileName);
            if(file.delete()){
                System.out.println(file.getName() + " is deleted!");
                result = true;
            } else{
                System.out.println("Delete operation is failed.");
            }
        } catch(Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public List<Integer> getFileKeys(){
        Enumeration<Integer> fileListEnum = DHTFragment.keys();
        List fileList=Collections.list(fileListEnum);
        return fileList;
    }

    public boolean sendItem(int k) {
        DhtMessage message = new DhtMessage(nodeId, successorId ,DhtMessage.ACTION_FILE_SEND, port, successorPort, DhtMessage.TYPE_SUCCESSOR, processSentPayload(k));
        queue.offer(message);
        // TODO Track error/ack
        removeFromDHT(k);
        return false;
    }

    public boolean isInInterval(int key, int fromId, int toId) {
        boolean result;
        // both interval bounds are equal -> calculate out of equals
        if (fromId == toId) {
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


    public String getPropFileName() {
        return propFileName;
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
            folder = Utils.getSafeProperty(prop, "folder");
            isSetup = Boolean.parseBoolean(Utils.getSafeProperty(prop, "isSetup"));
            if (isSetup) {
                isFirst = Boolean.parseBoolean(Utils.getSafeProperty(prop, "isFirst"));
                if (isFirst) {
                    String[] items = Utils.getSafeProperty(prop, "items").split(",");
                    for (String item : items) { // add initial items to dht
                        insertToDHT(item);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("nodeName: " + nodeName + " - port: " + port + " - referenceNodePort: " + referenceNodePort + " - isSetup:" + isSetup + " - isFirst:" + isFirst);
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public int getSuccessorPort() {
        return successorPort;
    }

    public void setSuccessorPort(int successorPort) {
        this.successorPort = successorPort;
    }

    public int getPredecessorId() {
        return predecessorId;
    }

    public void setPredecessorId(int predecessorId) {
        this.predecessorId = predecessorId;
    }

    public int getPredecessorPort() {
        return predecessorPort;
    }

    public void setPredecessorPort(int predecessorPort) {
        this.predecessorPort = predecessorPort;
    }

    public int getSuccessorId() {
        return successorId;
    }

    public void setSuccessorId(int successorId) {
        this.successorId = successorId;
    }

    public static Queue<DhtMessage> getQueue() {
        return queue;
    }
}
