package edu.ucsb.cs274.twophasecommit;

import edu.ucsb.cs274.paxos.Command;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.WriteObject;
import redis.clients.jedis.Jedis;

import java.io.BufferedInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class TwoPhaseCommit implements Runnable {

    int portNum;
    private Jedis jedis;
    int siteNum;
    HashMap<String, Long> locks;

    public static void main(String[] args) {
      //start the 3 sites
      for(int i=0; i<3; i++){
        (new Thread(new TwoPhaseCommit(i))).start();
      }

    }

    TwoPhaseCommit(int siteNum){
      this.siteNum = siteNum;
      locks = new HashMap<>();
    }


    public void run() {

        try {
          jedis = new Jedis("localhost", 6001 + siteNum);
          portNum = 8001 + siteNum;
          ServerSocket server = new ServerSocket(portNum);
          while (true) {
            Socket client = server.accept();
            client.setPerformancePreferences(0,2,1);
            client.setTcpNoDelay(true);
//            System.out.println("new client accepted");
            (new Thread(new RequestHandler(client))).start();
          }
        }
        catch (Exception e){
          e.printStackTrace();
        }
    }


    private class RequestHandler implements Runnable{

        Socket client;

        public RequestHandler(Socket client){
            this.client = client;
        }

        public void run(){
            try {
              ObjectInputStream reader = new ObjectInputStream(new BufferedInputStream(client.getInputStream()));
              ObjectOutputStream writer = new ObjectOutputStream(client.getOutputStream());

              WriteObject request = (WriteObject)reader.readObject();
//              System.out.println("In twoPC: " + " Txn id: " + request.getTransactionId() + " Command: " + request.getCommand());
              long txn = request.getTransactionId();

              for(Message m: request.getMessages()) {
                String key = m.getKey();
                char keyId = m.getKey().charAt(m.getKey().length()-1);
                int shardNo = (Integer.valueOf(keyId))%3;
                if(siteNum != shardNo )
                  continue;
                synchronized (locks) {
                  while(locks.containsKey(key)){
                    if(locks.get(key) >= txn){
                      break;
                    }
                    locks.wait();
                  }
                  locks.put(key, txn);
                }
              }

              writer.writeObject(new Message(Command.ACCEPT));
              writer.flush();
              Message response = (Message)reader.readObject();
              if(response.getCommand() == Command.ACCEPT){
                for(Message m: request.getMessages()){
                  char keyId = m.getKey().charAt(m.getKey().length()-1);
                  int shardNo = (Integer.valueOf(keyId))%3;
                  if(siteNum != shardNo )
                    continue;
                  synchronized (locks) {
                    locks.put(m.getKey(), txn);

//                    System.out.println("Key to be written is: " + m.getKey());
//                    System.out.println("Message is: " + m);
                    jedis.hmset(m.getKey(), m.getValues());
                  }
                }
                for(Message m: request.getMessages()){
                  char keyId = m.getKey().charAt(m.getKey().length()-1);
                  int shardNo = (Integer.valueOf(keyId))%3;
                  if(siteNum != shardNo )
                    continue;
                  synchronized (locks){
                    locks.remove(m.getKey());
                    locks.notifyAll();
                  }
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
