package com.isikun.firat.dht.simplified;

import com.google.gson.Gson;

import javax.xml.soap.Node;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DhtNode implements Serializable {

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

    private NodeRecord successor;
    private NodeRecord predecessor;
    private int nodeId;
//    private int predecessorId;
//    private int predecessorPort;
//    private int successorId;
//    private int successorPort;

    private static Queue<DhtMessage> messageQueue;
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
        messageQueue = new ConcurrentLinkedQueue<DhtMessage>();
        loadConfig();
        nodeId = Utils.getChecksum(nodeName, false);
        predecessor = new NodeRecord(nodeId, port);
        successor = new NodeRecord(nodeId, port);
//        predecessorId = nodeId;
//        successorId = nodeId;
//        successorPort = referenceNodePort;
//        predecessorPort = referenceNodePort;
        socketServer = new ServerThread(port);
        new Thread(socketServer).start();
        consumer = new QueueConsumer(messageQueue);
        new Thread(consumer).start();
        enterRing();
    }

    public NodeRecord succ(int k) {
        NodeRecord successorNode;
        if (nodeId == successor.getNodeId()) {
            successorNode = successor;
        } else if (k == nodeId) {
            successorNode = successor;
        } else if (isInInterval(k, predecessor.getNodeId(), successor.getNodeId())) {
            successorNode = successor;
        } else {
            //TODO BURDASIN
            DhtMessage successorQuery = new DhtMessage(nodeId, successor.getNodeId(), DhtMessage.ACTION_FIND, port, successor.getPort(), DhtMessage.TYPE_SUCCESSOR, k + "");
            System.out.println(successorQuery);
            DhtMessage response = successorQuery.sendMessage();
            System.out.println(response);
            System.out.println("PAYLOAD: " + response.getPayLoad());
            successorNode = NodeRecord.deserialize(response.getPayLoad());
        }
        return successorNode;
    }

    private int lookup(int k) {
        return 0;
    }

    private int insert(String itemName) {
        return 0;
    }

    private boolean enterRing() {
        boolean result = false;
        DhtMessage successorQuery = new DhtMessage(nodeId, successor.getNodeId(), DhtMessage.ACTION_ENTRY, port, referenceNodePort, DhtMessage.TYPE_SUCCESSOR, new NodeRecord(nodeId, port).serialize());
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
        DhtMessage response = null;
        System.out.println(message);
//        successorId = succ(Integer.parseInt(message.getPayLoad()));
        if (successor.getNodeId() == this.nodeId) { // if successor is not set optimization
            successor.setNodeId(message.getFromNodeId());
            successor.setPort(message.getFromPort());
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_PREDECESSOR, new NodeRecord(nodeId, port).serialize());
        } else if ((message.getFromNodeId() < successor.getNodeId()) && (message.getFromNodeId() > this.nodeId)) { //new successor without crossing 0
            successor = NodeRecord.deserialize(message.getPayLoad());
//            this.successorId = Integer.parseInt(message.getPayLoad());
//            this.successorPort = message.getPayloadOwnerPort();
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successor.serialize());
        } else { //Find the successor
            NodeRecord tempSuccessor = succ(message.getFromNodeId() + 1);
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, tempSuccessor.serialize());
        }
//        let our successor know we are her predecessor
//        response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_PREDECESSOR, nodeId + "");
        for (int key : getFileKeys()) {
            if (isInInterval(key, nodeId, successor.getNodeId())) {
                sendItem(key);
            }
        }
        return response;
    }

    public DhtMessage updatePredecessor(DhtMessage message) {
        // TODO same as updatesuccessor currently
        DhtMessage response;
        if (predecessor.getNodeId() == this.nodeId) { // if successor is not set optimization
            predecessor.setNodeId(message.getFromNodeId());
            predecessor.setPort(message.getFromPort());
            // let our successor know we are her successor
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, new NodeRecord(nodeId, port).serialize());
        } else if ((message.getFromNodeId() < successor.getNodeId()) && (message.getFromNodeId() > this.nodeId)) { //new successor without crossing 0
            predecessor.setPort(message.getFromPort());
            predecessor.setNodeId(message.getFromNodeId());
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, successor.serialize());
        } else { //Find the successor
            response = new DhtMessage(nodeId, message.getFromNodeId(), DhtMessage.ACTION_UPDATE, port, message.getFromPort(), DhtMessage.TYPE_SUCCESSOR, succ(message.getFromNodeId() + 1).serialize());

        }
        return response;
    }

    public DhtMessage processUpdateAction(DhtMessage request) {
        DhtMessage response = null;
        if (request.getAction() == DhtMessage.ACTION_UPDATE) {
            switch (request.getType()) {
                case DhtMessage.TYPE_PREDECESSOR:
                    NodeRecord tempPred= NodeRecord.deserialize(request.getPayLoad());
                    predecessor.setNodeId(tempPred.getNodeId());
                    System.out.println("Node " + nodeId + " predecessor is now " + predecessor.getNodeId());
                    response = new DhtMessage(nodeId, request.getFromNodeId(), DhtMessage.ACTION_ACK, port, request.getFromPort());
                    break;
                case DhtMessage.TYPE_SUCCESSOR:
                    NodeRecord tempSucc= NodeRecord.deserialize(request.getPayLoad());
                    successor.setNodeId(tempSucc.getNodeId());
                    System.out.println("Node " + nodeId + " successor is now " + successor.getNodeId());
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
}
