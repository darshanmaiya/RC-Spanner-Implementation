package edu.ucsb.cs274.twophasecommit;

import edu.ucsb.cs274.paxos.Command;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.WriteObject;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by alanbuzdar on 3/11/16.
 */
public class ClientRequestHandler implements Runnable {
  WriteObject wo;

  ClientRequestHandler(Socket request){
    try {
      ObjectInputStream readRequest = new ObjectInputStream(request.getInputStream());
      wo = (WriteObject) readRequest.readObject();
    }
    catch (Exception e){
      e.printStackTrace();
    }
  }

  public void run() {
    try {
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

      Message m1 = (Message) reader1.readObject();
      Message m2 = (Message) reader2.readObject();
      Message m3 = (Message) reader3.readObject();
      if (m1.getCommand() == Command.ACCEPT && m2.getCommand() == Command.ACCEPT && m3.getCommand() == Command.ACCEPT) {
        Message commit = new Message(Command.COMMIT);
        one.writeObject(commit);
        two.writeObject(commit);
        three.writeObject(commit);
        one.flush();
        two.flush();
        three.flush();
        reader1.readObject();
        reader2.readObject();
        reader3.readObject();
      }
      sOne.close();
      sTwo.close();
      sThree.close();
    }
    catch (Exception e){
      e.printStackTrace();
    }
  }
}

