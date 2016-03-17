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
	ObjectOutputStream ycsbWriter;
	ObjectInputStream ycsbReader;
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
			ycsbWriter = new ObjectOutputStream(request.getOutputStream());
			ycsbReader = new ObjectInputStream(request.getInputStream());

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

			Socket pLeaderOne = new Socket(properties.getProperty("paxosLeaderIp1"), paxosPort);
			Socket pLeaderTwo = new Socket(properties.getProperty("paxosLeaderIp2"), paxosPort);
			Socket pLeaderThree = new Socket(properties.getProperty("paxosLeaderIp3"), paxosPort);

			ObjectOutputStream paxosLeaderOneWriter = new ObjectOutputStream(pLeaderOne.getOutputStream());
			ObjectOutputStream paxosLeaderTwoWriter = new ObjectOutputStream(pLeaderTwo.getOutputStream());
			ObjectOutputStream paxosLeaderThreeWriter = new ObjectOutputStream(pLeaderThree.getOutputStream());

			ObjectInputStream paxosLeaderOneReader = new ObjectInputStream(pLeaderOne.getInputStream());
			ObjectInputStream paxosLeaderTwoReader = new ObjectInputStream(pLeaderTwo.getInputStream());
			ObjectInputStream paxosLeaderThreeReader = new ObjectInputStream(pLeaderThree.getInputStream());

			WriteObject ycsbMessage;
			WriteObject paxosOneMessage;
			WriteObject paxosTwoMessage;
			WriteObject paxosThreeMessage;

			while (true){

				// Read message from ycsb client
				ycsbMessage = (WriteObject)ycsbReader.readObject();

				// Check if READ or COMMIT
				if (ycsbMessage.getCommand() == Command.COMMIT){

					// Pass on the message to Paxos leaders
					paxosLeaderOneWriter.writeObject(ycsbMessage);
					paxosLeaderOneWriter.flush();
					paxosLeaderTwoWriter.writeObject(ycsbMessage);
					paxosLeaderTwoWriter.flush();
					paxosLeaderThreeWriter.writeObject(ycsbMessage);
					paxosLeaderThreeWriter.flush();
				}
				if (ycsbMessage.getCommand() == Command.READ){

					// First find which leader to contact 
					String key = ycsbMessage.getMessages().get(0).getKey();
					char keyId = key.charAt(key.length()-1);
					int shardNo = (Integer.valueOf(keyId))%3 + 1;

					if (shardNo == 1){
						paxosLeaderOneWriter.writeObject(ycsbMessage);
						paxosLeaderOneWriter.flush();

						paxosOneMessage = (WriteObject)paxosLeaderOneReader.readObject();

						ycsbWriter.writeObject(paxosOneMessage);
						ycsbWriter.flush();
					}
					else if (shardNo == 2){
						paxosLeaderTwoWriter.writeObject(ycsbMessage);
						paxosLeaderTwoWriter.flush();
						
						paxosTwoMessage = (WriteObject)paxosLeaderTwoReader.readObject();

						ycsbWriter.writeObject(paxosTwoMessage);
						ycsbWriter.flush();
					}
					else{
						paxosLeaderThreeWriter.writeObject(ycsbMessage);
						paxosLeaderThreeWriter.flush();
						
						paxosThreeMessage = (WriteObject)paxosLeaderThreeReader.readObject();

						ycsbWriter.writeObject(paxosThreeMessage);
						ycsbWriter.flush();
					}
				}
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
}

