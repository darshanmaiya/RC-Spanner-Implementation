package edu.ucsb.cs274.twophasecommit;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.Runnable;
import java.net.ServerSocket;
import java.net.Socket;
import edu.ucsb.cs274.paxos.*;
/**
 * Created by alanbuzdar on 2/28/16.
 */
public class Client {
    public static void main(String[] args){

        try {
           ServerSocket listener = new ServerSocket(8005);

            while(true) {
                Socket request = listener.accept();
                (new Thread(new ClientRequestHandler(request))).start();

            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}

