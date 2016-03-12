package edu.ucsb.cs274.twophasecommit;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.Runnable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

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

    private class ClientRequestHandler implements Runnable {
       WriteObject wo;

        ClientRequestHandler(Socket request){
            ObjectInputStream readRequest = new ObjectInputStream(request.getInputStream());
            wo = (WriteObject)readRequest.readObject();
        }

        public void run() {
            Socket sOne = new Socket("localhost", 8001);
            Socket sTwo = new Socket("localhost", 8002);
            Socket sThree = new Socket("localhost", 8003);

            ObjectOutputStream one = new ObjectOutputStream(sOne.getOutputStream());
            ObjectOutputStream two = new ObjectOutputStream(sTwo.getOutputStream());
            ObjectOutputStream three = new ObjectOutputStream(sThree.getOutputStream());

            ObjectInputStream reader1 = new ObjectInputStream(sOne.getInputStream());
            ObjectInputStream reader2 = new ObjectInputStream(sTwo.getInputStream());
            ObjectInputStream reader3 = new ObjectInputStream(sThree.getInputStream());

            one.writeObject(wo);
            one.flush();
            two.writeObject(wo);
            two.flush();
            three.writeObject(wo);
            three.flush();

            Message m1 = (Message)reader1.readObject();
            Message m2 = (Message)reader2.readObject();
            Message m3 = (Message)reader3.readObject();
            if (m1.getCommand() == ACCEPT && m2.getCommand() == ACCEPT && m3.getCommand() == ACCEPT) {
                Message commit = new Message(COMMIT);
                one.writeObject(commit);
                two.println(commit);
                three.println(commit);
                one.flush();
                two.flush();
                three.flush();
                m1 = (Message)reader1.readObject();
                m2 = (Message)reader2.readObject();
                m3 = (Message)reader3.readObject();
            }
            sOne.close();
            sTwo.close();
            sThree.close();
        }
    }
}
