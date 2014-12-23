package com.isikun.firat.dht.simplified;

import com.google.gson.Gson;

import javax.xml.soap.Node;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DhtNode implements Serializable {

    public static final String HASH_ALGORITHM = "SHA-1";
    public static final int KEY_SPACE = 5;
    public static final int MAX_NODES = (int)Math.pow(KEY_SPACE, 2);
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

    private static NodeRecord successor;
    private static NodeRecord predecessor;
    private int nodeId;

    private static Queue<DhtMessage> messageQueue;
    private static QueueConsumer consumer;

    private ServerThread socketServer;
    //Singleton pattern
    private static DhtNode INSTANCE;

    public synchronized static DhtNode getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DhtNode();
        }
        return INSTANCE;
    }

    private DhtNode() {
        DHTFragment = new Hashtable<Integer, String>();
        messageQueue = new ConcurrentLinkedQueue<DhtMessage>();
        loadConfig();
        nodeId = Utils.getChecksum(nodeName, false);
        System.out.println("=== Node ID: " + nodeId + "===");
        predecessor = new NodeRecord(nodeId, port);
        successor = new NodeRecord(nodeId, port);
        socketServer = new ServerThread(port);
        new Thread(socketServer).start();
        consumer = new QueueConsumer(messageQueue);
        new Thread(consumer).start();
        if(!isFirst){
            enterRing();
        }
    }

    public synchronized NodeRecord succ(int k) {
        NodeRecord successorNode;
        System.out.println("SUCC\nPREDECESSOR:" + predecessor);
        System.out.println("SUCCESSOR:" + successor);
        System.out.println("K:" + k);
        if (nodeId == successor.getNodeId()) { // Init optimization
            System.out.println("SUCCC1");
            successorNode = successor;
        } else if (k == nodeId) {
            System.out.println("SUCCC2");
            successorNode = successor;
        } else if (isInInterval(k, nodeId, successor.getNodeId())) {
            System.out.println("SUCCC3");
            successorNode = successor;
        } else {
            System.out.println("SUCCC5");
            //TODO BURDASIN
            DhtMessage successorQuery = new DhtMessage(nodeId, successor.getNodeId(), DhtMessage.ACTION_FIND, port, successor.getPort(), DhtMessage.TYPE_SUCCESSOR, k + "");
            System.out.println(successorQuery);
            DhtMessage response = successorQuery.sendMessage();
            System.out.println(response);
            System.out.println("PAYLOAD: " + response.getPayLoad());
            successorNode = response.payloadToNodeRecord();
        }
        return successorNode;
    }

    public NodeRecord pred(){
        return predecessor;
    }

    private int lookup(int k) {
        return 0;
    }

    private int insert(String itemName) {
        return 0;
    }

    private boolean enterRing() {
        boolean result = false;
        DhtMessage initQuery = new DhtMessage(nodeId, successor.getNodeId(), DhtMessage.ACTION_ENTRY, port, referenceNodePort, DhtMessage.TYPE_SUCCESSOR, new NodeRecord(nodeId, port).serialize());
        DhtMessage response = initQuery.sendMessage();
        processUpdateAction(response);
        return result;
    }

    private int leaveRing() {

        return 0;
    }

    private int stabilize() {
        // pred(succ(this.getNodeId() + 1))
        return 0;
    }

    public synchronized DhtMessage bootstrapNode(DhtMessage message){
        DhtMessage response;
        NodeRecord successor = succ(message.getFromNodeId() + 1);
        response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successor.serialize());
        return response;
    }

    public boolean updatePredecessor(NodeRecord potentialPredecessor){
        boolean result = false;
        System.out.println("this.toNodeRecord().compareTo(predecessor): " + this.toNodeRecord().compareTo(predecessor));
        if (isInInterval(potentialPredecessor.getNodeId(), predecessor.getNodeId(), nodeId)){
            predecessor = potentialPredecessor;
            result = true;
        } else if (this.toNodeRecord().compareTo(predecessor) == 0){
            predecessor = potentialPredecessor;
            result = true;
        }
        return result;
    }

    public boolean updateSuccessor(NodeRecord potentialSuccessor){
        boolean result = false;
        System.out.println("this.toNodeRecord().compareTo(successor): " + this.toNodeRecord().compareTo(successor));
        if (isInInterval(potentialSuccessor.getNodeId(), nodeId, successor.getNodeId())){
            successor = potentialSuccessor;
            result = true;
        } else if (this.toNodeRecord().compareTo(successor) == 0){
            successor = potentialSuccessor;
            result = true;
        }
        return result;
    }

    public synchronized DhtMessage processUpdateAction(DhtMessage request) {
        DhtMessage response = null;
        if (request.getAction() == DhtMessage.ACTION_UPDATE) {
            switch (request.getType()) {
                case DhtMessage.TYPE_PREDECESSOR:
                    NodeRecord prevPredecessor = predecessor;
                    if (updatePredecessor(request.payloadToNodeRecord())){
                        System.out.println("Node " + nodeId + " predecessor is now " + predecessor.getNodeId());
                        // Let our previous precessor know its new successor
                        DhtMessage updatePredecessor;
                        if(predecessor.compareTo(this.toNodeRecord()) == 0){
                            updatePredecessor = new DhtMessage(nodeId, prevPredecessor.getNodeId(), DhtMessage.ACTION_UPDATE, port, prevPredecessor.getPort(), DhtMessage.TYPE_SUCCESSOR, predecessor.serialize());
                        } else { // Initialization handling
                            updatePredecessor = new DhtMessage(nodeId, predecessor.getNodeId(), DhtMessage.ACTION_UPDATE, port, predecessor.getPort(), DhtMessage.TYPE_SUCCESSOR, prevPredecessor.serialize());
                        }
                        messageQueue.offer(updatePredecessor);
                    }
                    response = new DhtMessage(nodeId, request.getFromNodeId(), DhtMessage.ACTION_ACK, port, request.getFromPort());
                    break;
                case DhtMessage.TYPE_SUCCESSOR:
                    NodeRecord prevSuccessor = successor;
                    if (updateSuccessor(request.payloadToNodeRecord())){
                        System.out.println("Node " + nodeId + " successor is now " + successor.getNodeId());
                        // Let our previous successor know its new pred
                        DhtMessage updateSuccessor;
                        if(successor.compareTo(this.toNodeRecord()) == 0) {
                            updateSuccessor = new DhtMessage(nodeId, prevSuccessor.getNodeId(), DhtMessage.ACTION_UPDATE, port, prevSuccessor.getPort(), DhtMessage.TYPE_PREDECESSOR, successor.serialize());
                        } else { //Initialization handling
                            updateSuccessor = new DhtMessage(nodeId, successor.getNodeId(), DhtMessage.ACTION_UPDATE, port, successor.getPort(), DhtMessage.TYPE_PREDECESSOR, prevSuccessor.serialize());
                        }
                        messageQueue.offer(updateSuccessor);
                        for (int key : getFileKeys()) {
                            if (isInInterval(key, nodeId, successor.getNodeId())) {
                                sendItem(key);
                            }
                        }
                    }
                    response = new DhtMessage(nodeId, request.getFromNodeId(), DhtMessage.ACTION_ACK, port, request.getFromPort());
                    break;
                default:
                    response = new DhtMessage(nodeId, request.getFromNodeId(), DhtMessage.ACTION_ERROR, port, request.getFromPort());
                    break;
            }
        }
        System.out.println("SUCCESSOR:" + successor);
        System.out.println("PRECESSOR:" + predecessor);
        return response;
    }

    public boolean isInInterval(int key, int fromId, int toId) {
        System.out.println("key:" + key + " from:"+ fromId +" to:" + toId);
        boolean result;
        // both interval bounds are equal -> calculate out of equals
        if (fromId == toId) {
            System.out.println("EQUAL INTERVAL");
            result = (key != fromId);
        }

        else if (key == fromId){
            System.out.println("KEY is equal with bounds");
            result = true;
        }

        // interval does not cross zero -> compare with both bounds
        else if (fromId < toId) {
            System.out.println("fromid<toid");
            result = ((key > fromId) && (key < toId));
        }

        // interval crosses zero -> split interval at zero
        else {
            boolean lowerInterval = ((key > fromId) && (key <= MAX_ID));
            System.out.println("LOWERINTERVAL: " + lowerInterval);
            boolean lowerTerminationCondition = (fromId != MAX_ID);
            System.out.println("LOWERTERMINATION: " + lowerTerminationCondition);
            boolean upperInterval = ((key >= 0) && (key < toId));
            System.out.println("UPPER INTERVAL: " + upperInterval);
            boolean upperTerminationCondition = (0 != toId);
            System.out.println("UPPER TERMINATION: " + upperTerminationCondition);

            result = ((lowerInterval && lowerTerminationCondition) || (upperInterval && upperTerminationCondition));
            System.out.println("FINAL: " + result);
        }

        return result;
    }

    private void insertToDHT(String itemName) {
        int itemId = Utils.getChecksum(folder + "/" + itemName, true);
//        System.out.println("itemName: " + itemName + " - itemId: " + itemId);
        DHTFragment.put(itemId, itemName);
    }

    private void removeFromDHT(int itemId) {
        if (deleteFile(DHTFragment.get(itemId))) {
            DHTFragment.remove(itemId);
        }
    }

    public String processSentPayload(int key) {
        Gson gson = new Gson();
        String fileName = DHTFragment.get(key);
        String fileContents = Utils.encodePayload(readFile(fileName));
        String[] payload = {fileName, fileContents};
        String processedPayload = gson.toJson(payload);
        return processedPayload;
    }

    public boolean processReceivedPayload(String processedPayload) {
        boolean result = false;
        Gson gson = new Gson();
        String[] payload = gson.fromJson(processedPayload, String[].class);
        if (saveFile(payload)) {
            insertToDHT(payload[0]);
            System.out.println(payload[0] + " : " + payload[1] + " is saved");
            result = true;
        }
        return result;
    }

    public String readFile(String fileName) {
        String fileContents = null;
        try (BufferedReader br = new BufferedReader(new FileReader(folder + "/" + fileName))) {
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

    public boolean saveFile(String[] payload) {
        boolean result = false;
        try {
            PrintWriter writer = new PrintWriter(folder + "/" + payload[0], "UTF-8");
            writer.println(Utils.decodePayload(payload[1]));
            writer.close();
            result = true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean deleteFile(String fileName) {
        boolean result = false;
        try {
            File file = new File(folder + "/" + fileName);
            if (file.delete()) {
                System.out.println(file.getName() + " is deleted!");
                result = true;
            } else {
                System.out.println("Delete operation is failed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Integer> getFileKeys() {
        Enumeration<Integer> fileListEnum = DHTFragment.keys();
        List fileList = Collections.list(fileListEnum);
        return fileList;
    }

    public boolean sendItem(int k) {
        DhtMessage message = new DhtMessage(nodeId, successor.getNodeId(), DhtMessage.ACTION_FILE_SEND, port, successor.getPort(), DhtMessage.TYPE_SUCCESSOR, processSentPayload(k));
        messageQueue.offer(message);
        // TODO Track error/ack
        removeFromDHT(k);
        return false;
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

    public NodeRecord getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(NodeRecord predecessor) {
        this.predecessor = predecessor;
    }

    public NodeRecord getSuccessor() {
        return successor;
    }

    public void setSuccessor(NodeRecord successor) {
        this.successor = successor;
    }

    public static Queue<DhtMessage> getMessageQueue() {
        return messageQueue;
    }

    public NodeRecord toNodeRecord(){
        return new NodeRecord(this.getNodeId(), this.getPort());
    }
}
