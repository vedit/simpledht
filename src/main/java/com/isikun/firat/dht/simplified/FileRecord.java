package com.isikun.firat.dht.simplified;

import java.io.*;

import com.google.gson.Gson;

/**
 * Created by hexenoid on 11/24/14.
 */
public class FileRecord implements Serializable {

    String fileName;
    String fileContents;

    public FileRecord() {
    }

    public FileRecord(String fileName, String fileContents) {
        this.fileName = fileName;
        this.fileContents = fileContents;
    }

    public FileRecord(String fileName, String folder, boolean hack){
        this.fileName = fileName;
        this.fileContents = readFile(fileName, folder);
    }

    public String readFile(String fileName, String folder) {
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

    public boolean saveFile(String folder) {
        boolean result = false;
        try {
            PrintWriter writer = new PrintWriter(folder + "/" + this.getFileName(), "UTF-8");
            writer.println(Utils.decodePayload(this.getFileContents()));
            writer.close();
            result = true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean deleteFile(String folder) {
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

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileContents() {
        return fileContents;
    }

    public void setFileContents(String fileContents) {
        this.fileContents = fileContents;
    }

    public String serialize(){
        Gson gson = new Gson();
        String json = gson.toJson(this);
//        System.out.println(json);
        return json;
    }

    public static FileRecord deserialize(String fileRecord){
        Gson gson = new Gson();
//        System.out.println(message);
        return gson.fromJson(fileRecord, FileRecord.class);
    }
}
