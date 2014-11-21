package com.isikun.firat.dht.simplified;

public class Main {

    public static void main(String[] args) {
        DhtNode dht1 = DhtNode.getInstance();
        for (int i = 0; i < args.length; i++) {
            System.out.println(args[i]);
        }
//        try {
//            int result = Utils.getChecksum(".gitignore", true);
//            System.out.println(result);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
