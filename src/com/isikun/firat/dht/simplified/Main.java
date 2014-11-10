package com.isikun.firat.dht.simplified;

public class Main {

    public static void main(String[] args) {
	    DhtNode dht1 = new DhtNode();
        try {
            int result = dht1.getChecksum(".gitignore", true);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
