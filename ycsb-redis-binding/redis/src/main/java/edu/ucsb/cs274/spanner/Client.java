package edu.ucsb.cs274.spanner;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.Runnable;
import java.net.ServerSocket;
import java.net.Socket;
import edu.ucsb.cs274.paxos.*;

public class Client {
    public static void main(String[] args){

        try {
           ServerSocket listener = new ServerSocket(5000);
           
            while(true) {
                Socket request = listener.accept();
                (new Thread(new TwoPCClientRequestHandler(request))).start();
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}

