package com.isikun.firat.dht.simplified;

import org.apache.commons.codec.binary.Base64;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Random;
import java.security.SecureRandom;

public class Utils {

    public static String getSafeProperty(Properties prop, String propName) throws Exception {
        String property = prop.getProperty(propName);
        if (property == null) {
            throw new Exception(propName + " is null");
        }
        return property;
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

    public static String encodePayload(String payload){
        byte[] encodedBytes = Base64.encodeBase64(payload.getBytes());
        String encodedPayload = new String(encodedBytes);
        System.out.println("encodedBytes: " + encodedPayload);
        return encodedPayload;
    }

    public static String decodePayload(String encodedBytes){
        byte[] decodedPayload = Base64.decodeBase64(encodedBytes);
        String payload = new String(decodedPayload);
        System.out.println("decodedBytes: " + payload);
        return payload;
    }

    public static int randInt(int min, int max) {

        // NOTE: Usually this should be a field rather than a method
        // variable so that it is not re-seeded every call.
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }


    public static String randomHash() {
        SecureRandom random = new SecureRandom();
        return new BigInteger(130, random).toString(32);
    }
}
