package edu.ucsb.cs274.twophasecommit;

import edu.ucsb.cs274.paxos.Command;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.WriteObject;
import redis.clients.jedis.Jedis;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

public class TwoPhaseCommit {

    int portNum;
    private Jedis jedis;


    public static void main(String[] args) {
        TwoPhaseCommit tpc = new TwoPhaseCommit();
        try {
            tpc.initialize();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }


    public void initialize() throws Exception{
        jedis = new Jedis("localhost", 6001);
        portNum = 8001;
        ServerSocket server = new ServerSocket(portNum);
        while(true){
            Socket client = server.accept();
            System.out.println("new client accepted");
            (new Thread(new RequestHandler(client))).start();
        }
    }


    private class RequestHandler implements Runnable{

        Socket client;

        public RequestHandler(Socket client){
            this.client = client;
        }

        public void run(){
            try {
              ObjectInputStream reader = new ObjectInputStream(client.getInputStream());
              ObjectOutputStream writer = new ObjectOutputStream(client.getOutputStream());

              WriteObject request = (WriteObject)reader.readObject();
              System.out.println("In twoPC: " + " Txn id: " + request.getTransactionId() + " Command: " + request.getCommand());

              writer.writeObject(new Message(Command.ACCEPT));
              writer.flush();
              Message response = (Message)reader.readObject();
              if(response.getCommand() == Command.ACCEPT){
                for(Message m: request.getMessages()){
                	System.out.println("Key to be written is: " + m.getKey());
                	System.out.println("Message is: " + m);
                	jedis.hmset(m.getKey(), m.getValues());
                }
                writer.writeObject(new Message(Command.SUCCESS));
                writer.flush();
              }
              else{
                writer.writeObject(new Message(Command.FAILURE));
                writer.flush();
              }


            } catch (Exception e){
                e.printStackTrace();
            }

        }
    }
}
