package com.isikun.firat.dht.simplified;

import com.google.gson.Gson;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DhtNode implements Serializable {

    public static final String HASH_ALGORITHM = "SHA-1";
    public static final int KEY_SPACE = 5;
    public static final int MAX_NODES = (int) Math.pow(KEY_SPACE, 2);
    public static final int MAX_ID = MAX_NODES - 1;
    private final int STREAM_BUFFER_SIZE = 8192;
    private volatile Hashtable<Integer, String> DHTFragment;

    private String nodeName;
    private int port;
    private int referenceNodePort;
    private boolean isSetup;
    private boolean isFirst;
    private String folder;

    private static String propFileName = System.getProperty("configFile", "config1.properties");

    private static volatile FingerTable fingerTable;
    private static NodeRecord predecessor;
    private int nodeId;

    private static Queue<DhtMessage> messageQueue;
    private static QueueConsumer consumer;

    private static Stabilizer stabilizer;

    private volatile ServerThread socketServer;
    //Singleton pattern
    private static volatile DhtNode instance = null;

    public static DhtNode getInstance() {
        if (instance == null) {
            synchronized (DhtNode.class) {
                if (instance == null) {
                    instance = new DhtNode();
                }
            }
        }
        return instance;
    }

    private DhtNode() {
        DHTFragment = new Hashtable<Integer, String>();
        messageQueue = new ConcurrentLinkedQueue<DhtMessage>();
        loadConfig();
        nodeId = Utils.getChecksum(nodeName, false);
        System.out.println("=== Node ID: " + nodeId + "===");
        predecessor = new NodeRecord(nodeId, port);
        synchronized (DhtNode.class) {
            new java.util.Timer().schedule(
                    new java.util.TimerTask() {
                        @Override
                        public void run() {
                            socketServer = ServerThread.getInstance();
//                        new Thread(socketServer).start();
                            consumer = new QueueConsumer(messageQueue);
                            new Thread(consumer).start();
                            fingerTable = new FingerTable();
                            stabilizer = new Stabilizer();
                            new Thread(stabilizer).start();
                            if (!isFirst) {
                                enterRing();
                            }
                        /*
                        HORRIBLE HORRIBLE HACK
                        to avoid cyclic dependency problems
                        */
                        }
                    },
                    1000
            );
        }
    }

    public synchronized NodeRecord succ(int k) {
        NodeRecord successorNode;
        NodeRecord finger = fingerTable.getClosestPrecedingNode(k);
        System.out.println("SUCC\nPREDECESSOR:" + predecessor);
        System.out.println("SUCCESSOR:" + fingerTable);
        System.out.println("FINGER:" + finger);
        System.out.println("K:" + k);
        if (this.toNodeRecord().compareTo(fingerTable.getFirst()) == 0 && this.toNodeRecord().compareTo(predecessor) == 0) { // Init optimization
            System.out.println("SUCCC1");
            successorNode = finger;
        } else if (k == nodeId) {
            System.out.println("SUCCC2");
            successorNode = this.toNodeRecord();
        } else if (k == finger.getNodeId()) {
            successorNode = finger;
        } else if (Utils.isInInterval(k, nodeId, finger.getNodeId())) {
            System.out.println("SUCCC3");
            successorNode = finger;
        } else {
            System.out.println("SUCCC5");
            DhtMessage successorQuery = DhtMessage.findSuccessor(finger, k + "");
            System.out.println(successorQuery);
            DhtMessage response = successorQuery.sendMessage();
            System.out.println(response);
            System.out.println("PAYLOAD: " + response.getPayLoad());
            successorNode = response.payloadToNodeRecord();
        }
        return successorNode;
    }

    private boolean enterRing() {
        boolean result = false;
        DhtMessage initQuery = DhtMessage.makeEntry(referenceNodePort);
        DhtMessage response = initQuery.sendMessage();
        fingerTable.stabilize();
        processUpdateAction(response);
        return result;
    }

    public synchronized DhtMessage bootstrapNode(DhtMessage message) {
        DhtMessage response;
        NodeRecord successor = succ(message.getFromNodeId() + 1);
        response = DhtMessage.updateSuccessor(message.toNodeRecord(), successor.serialize());
        return response;
    }

    public boolean updatePredecessor(NodeRecord potentialPredecessor) {
        boolean result = false;
        System.out.println("this.toNodeRecord().compareTo(predecessor): " + this.toNodeRecord().compareTo(predecessor));
        if (Utils.isInInterval(potentialPredecessor.getNodeId(), predecessor.getNodeId(), nodeId)) {
            predecessor = potentialPredecessor;
            result = true;
        } else if (this.toNodeRecord().compareTo(predecessor) == 0) {
            predecessor = potentialPredecessor;
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
                    if (updatePredecessor(request.payloadToNodeRecord())) {
                        System.out.println("Node " + nodeId + " predecessor is now " + predecessor.getNodeId());
                        // Let our previous precessor know its new successor
                        DhtMessage updatePredecessor;
                        if (predecessor.compareTo(this.toNodeRecord()) == 0) {
                            updatePredecessor = DhtMessage.updateSuccessor(prevPredecessor, predecessor.serialize());
                        } else { // Initialization handling
                            updatePredecessor = DhtMessage.updateSuccessor(predecessor, prevPredecessor.serialize());
                        }
                        messageQueue.offer(updatePredecessor);
                    }
                    response = DhtMessage.sendAck(request.toNodeRecord());
                    break;
                case DhtMessage.TYPE_SUCCESSOR:
                    NodeRecord finger = fingerTable.getFirst();
                    NodeRecord prevSuccessor = fingerTable.getFirst();
                    System.out.println("Node " + nodeId + " successor is now " + finger.getNodeId());
                    // Let our previous successor know its new pred
                    DhtMessage updateSuccessor;
                    fingerTable.insert(request.payloadToNodeRecord(), fingerTable.getClosestPrecedingNode(request.payloadToNodeRecord().getNodeId()).getNodeId());
                    if (finger.compareTo(this.toNodeRecord()) == 0) {
                        updateSuccessor = DhtMessage.updatePredecessor(prevSuccessor, finger.serialize());
                    } else { //Initialization handling
                        updateSuccessor = DhtMessage.updatePredecessor(finger, prevSuccessor.serialize());
                    }
                    messageQueue.offer(updateSuccessor);
                    for (int key : getFileKeys()) {
                        if (Utils.isInInterval(key, nodeId, finger.getNodeId())) {
                            sendItem(key);
                        }
                    }
                    response = DhtMessage.sendAck(request.toNodeRecord());
                    break;
                default:
                    response = DhtMessage.sendError(request.toNodeRecord());
                    break;
            }
        }
        System.out.println("SUCCESSOR:" + fingerTable);
        System.out.println("PRECESSOR:" + predecessor);
        return response;
    }

    public void checkIfNeighbor(DhtMessage message) {
        NodeRecord potentialNeighbor = message.toNodeRecord();
        if (!fingerTable.Contains(potentialNeighbor)) {
            if (this.toNodeRecord().compareTo(predecessor) == 0 && this.toNodeRecord().compareTo(fingerTable.getFirst()) == 0) {
                getMessageQueue().offer(DhtMessage.updateSuccessor(this.toNodeRecord(), potentialNeighbor.serialize()));
                getMessageQueue().offer(DhtMessage.updatePredecessor(this.toNodeRecord(), potentialNeighbor.serialize()));
            }
        }
    }

    private void insertToDHT(String itemName) {
        int itemId = Utils.getChecksum(folder + "/" + itemName, true);
        DHTFragment.put(itemId, itemName);
    }

    private void removeFromDHT(int itemId) {
        if (Utils.deleteFile(DHTFragment.get(itemId), folder)) {
            DHTFragment.remove(itemId);
        }
    }

    public String processSentPayload(int key) {
        Gson gson = new Gson();
        String fileName = DHTFragment.get(key);
        String fileContents = Utils.encodePayload(Utils.readFile(fileName, folder));
        String[] payload = {fileName, fileContents};
        String processedPayload = gson.toJson(payload);
        return processedPayload;
    }

    public boolean processReceivedPayload(String processedPayload) {
        boolean result = false;
        Gson gson = new Gson();
        String[] payload = gson.fromJson(processedPayload, String[].class);
        if (Utils.saveFile(payload, folder)) {
            insertToDHT(payload[0]);
            System.out.println(payload[0] + " : " + payload[1] + " is saved");
            result = true;
        }
        return result;
    }

    public List<Integer> getFileKeys() {
        Enumeration<Integer> fileListEnum = DHTFragment.keys();
        return Collections.list(fileListEnum);
    }

    public boolean sendItem(int k) {
        NodeRecord successor = succ(k);
        DhtMessage message = DhtMessage.sendFile(successor, processSentPayload(k));
        messageQueue.offer(message);
        // TODO Track error/ack
        removeFromDHT(k);
        return false;
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
        DhtNode.predecessor = predecessor;
    }

    public NodeRecord getSuccessor() {
        return fingerTable.getFirst();
    }

    public String getPropFileName() {
        return propFileName;
    }

    public static Queue<DhtMessage> getMessageQueue() {
        return messageQueue;
    }

    public NodeRecord toNodeRecord() {
        return new NodeRecord(this.getNodeId(), this.getPort());
    }

    public static FingerTable getFingerTable() {
        return fingerTable;
    }
}
