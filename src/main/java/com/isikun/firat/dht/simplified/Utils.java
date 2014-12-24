package com.isikun.firat.dht.simplified;

import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

public class Utils {

    public static String getSafeProperty(Properties prop, String propName) throws Exception {
        String property = prop.getProperty(propName);
        if (property == null) {
            throw new Exception(propName + " is null");
        }
        return property;
    }

    public static String readFile(String fileName, String folder) {
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

    public static boolean saveFile(String[] payload, String folder) {
        boolean result = false;
        try {
            PrintWriter writer = new PrintWriter(folder + "/" + payload[0], "UTF-8");
            writer.println(Utils.decodePayload(payload[1]));
            writer.close();
            result = true;
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static boolean deleteFile(String fileName, String folder) {
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


    public static boolean isInInterval(int key, int fromId, int toId) {
        System.out.println("key:" + key + " from:" + fromId + " to:" + toId);
        boolean result;
        // both interval bounds are equal -> calculate out of equals
        if (fromId == toId) {
            System.out.println("EQUAL INTERVAL");
            result = (key != fromId);
        } else if (key == fromId) {
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
            boolean lowerInterval = ((key > fromId) && (key <= DhtNode.MAX_ID));
            System.out.println("LOWERINTERVAL: " + lowerInterval);
            boolean lowerTerminationCondition = (fromId != DhtNode.MAX_ID);
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

    public static int getChecksum(String input, boolean isFile) {
        int result;
        byte[] checksum = null;
        if (isFile) {
            try {
                checksum = createFileChecksum(input);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                checksum = createStringChecksum(input);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        result = Math.abs(new BigInteger(checksum).intValue()) % DhtNode.MAX_NODES;
        return result;
    }


    public static byte[] createStringChecksum(String input) throws Exception {
        byte[] result;
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(DhtNode.HASH_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (md != null) {
            md.update(input.getBytes());
            result = md.digest();
        } else {
            throw new Exception("Message Digest is null");
        }
        return result;
    }

    public static byte[] createFileChecksum(String filename) throws Exception {
        byte[] result;
        InputStream fis = null;
        try {
            fis = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        byte[] buffer = new byte[1024];
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(DhtNode.HASH_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int numRead = -1;
        do {
            if (fis != null) {
                try {
                    numRead = fis.read(buffer);
                    if (numRead > 0 && md != null) {
                        md.update(buffer, 0, numRead);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } while (numRead != -1);
        try {
            if (fis != null) {
                fis.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (md != null) {
            result = md.digest();
        } else {
            throw new Exception("Message Digest is null");
        }
        return result;
    }

    public static String encodePayload(String payload) {
        byte[] encodedBytes = Base64.encodeBase64(payload.getBytes());
        String encodedPayload = new String(encodedBytes);
        System.out.println("encodedBytes: " + encodedPayload);
        return encodedPayload;
    }

    public static String decodePayload(String encodedBytes) {
        byte[] decodedPayload = Base64.decodeBase64(encodedBytes);
        String payload = new String(decodedPayload);
        System.out.println("decodedBytes: " + payload);
        return payload;
    }
}
