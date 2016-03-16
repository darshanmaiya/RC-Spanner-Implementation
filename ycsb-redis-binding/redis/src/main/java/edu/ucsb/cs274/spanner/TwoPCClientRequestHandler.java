package edu.ucsb.cs274.spanner;

import edu.ucsb.cs274.paxos.Command;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.WriteObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Properties;

public class TwoPCClientRequestHandler implements Runnable {
	WriteObject wo;
	ObjectOutputStream paxosOut;
	ObjectInputStream paxosIn;
	Socket request;
	TwoPCClientRequestHandler(Socket request){
		try {
			this.request = request;
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	public void run() {
		try {
			paxosOut = new ObjectOutputStream(request.getOutputStream());
			paxosIn = new ObjectInputStream(request.getInputStream());
			Object x =  paxosIn.readObject();
			if(x instanceof Message){
				Message m = (Message)x;
			}
			else{
				wo = (WriteObject)x;
			}
			// Read Config file, Change IP address and port. These are Paxos leaders
			Properties properties = new Properties();
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			InputStream is = classloader.getResourceAsStream("config.properties");
			try {
				properties.load(is);
			} catch (IOException e) {	
				e.printStackTrace();
				return;
			}
			
			// Connect to Paxos Leaders
			int paxosPort = Integer.valueOf(properties.getProperty("paxosLeaderPort"));
			
			Socket paxosLeaderOne = new Socket(properties.getProperty("paxosLeaderIp1"), paxosPort);
			Socket paxosLeaderTwo = new Socket(properties.getProperty("paxosLeaderIp2"), paxosPort);
			Socket paxosLeaderThree = new Socket(properties.getProperty("paxosLeaderIp3"), paxosPort);

			ObjectOutputStream one = new ObjectOutputStream(paxosLeaderOne.getOutputStream());
			ObjectOutputStream two = new ObjectOutputStream(paxosLeaderTwo.getOutputStream());
			ObjectOutputStream three = new ObjectOutputStream(paxosLeaderThree.getOutputStream());

			ObjectInputStream reader1 = new ObjectInputStream(paxosLeaderOne.getInputStream());
			ObjectInputStream reader2 = new ObjectInputStream(paxosLeaderTwo.getInputStream());
			ObjectInputStream reader3 = new ObjectInputStream(paxosLeaderThree.getInputStream());

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
				paxosOut.writeObject(commit);
				paxosOut.flush();
				commit = (Message)paxosIn.readObject();
				one.writeObject(commit);
				two.writeObject(commit);
				three.writeObject(commit);
				one.flush();
				two.flush();
				three.flush();
				m1 = (Message)reader1.readObject();
				m2 = (Message)reader2.readObject();
				m3 = (Message)reader3.readObject();
				if(m1.getCommand() == Command.SUCCESS && m2.getCommand() == Command.SUCCESS && m3.getCommand() == Command.SUCCESS)
					paxosOut.writeObject(new Message(Command.SUCCESS));
				else
					paxosOut.writeObject(new Message(Command.FAILURE));
				paxosOut.flush();
			}

			paxosLeaderOne.close();
			paxosLeaderTwo.close();
			paxosLeaderThree.close();

		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
}

