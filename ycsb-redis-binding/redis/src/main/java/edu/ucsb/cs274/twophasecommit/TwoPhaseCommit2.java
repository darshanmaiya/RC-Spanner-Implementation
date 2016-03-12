package edu.ucsb.cs274.twophasecommit;

import redis.clients.jedis.Jedis;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

public class TwoPhaseCommit2 {

    ArrayList<String> locks;
    int portNum;
    private Jedis jedis;


    public static void main(String[] args) {
        TwoPhaseCommit2 tpc = new TwoPhaseCommit2();
        try {
            tpc.initialize();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }



    public void initialize() throws Exception{
        locks = new ArrayList<String>();
        jedis = new Jedis("localhost", 6002);
        portNum = 8002;
        ServerSocket server = new ServerSocket(portNum);
        while(true){
            Socket client = server.accept();
            System.out.println("new client accepted");
            (new Thread(new RequestHandler(client))).start();
        }
    }

    public synchronized boolean getLock(String key){
        if(locks.contains(key)){
            return false;
        }
        locks.add(key);
        return true;
    }

    public synchronized void freeLock(String key){
        locks.remove(key);
        key.intern().notify();
    }


    private class RequestHandler implements Runnable{

        Socket client;

        public RequestHandler(Socket client){
            this.client = client;
        }

        public void run(){
            try {
                Scanner reader = new Scanner(client.getInputStream());
                String request = reader.nextLine();
                String key = request.split(" ")[0];
                String value = request.split(" ")[1];
                while (!getLock(key)) {
                    synchronized (key.intern()) {
                        key.intern().wait();
                    }
                }

                PrintWriter out = new PrintWriter(client.getOutputStream());

                out.println("Accept");
                out.flush();

                String commit = reader.nextLine();

                if(commit.contains("Commit")){
                    jedis.set(key, value);

                }


                freeLock(key);
                out.println("Done");
                out.flush();

            } catch (Exception e){
                e.printStackTrace();
            }

        }
    }
}
