package edu.ucsb.cs274.spanner;

import edu.ucsb.cs274.paxos.Command;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.WriteObject;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;

public class PaxosLeader{

	int portNum;
	private Jedis jedis;
	HashMap<String, Long> locks;


	PaxosLeader(){
		locks = new HashMap<>();
	}

	public static void main(String[] args) {

		PaxosLeader paxosLeader = new PaxosLeader();

		// Open port for connection
		try {
			ServerSocket server = new ServerSocket(8000);
			while (true) {
				Socket client = server.accept();
				(new Thread(paxosLeader.new RequestHandler(client))).start();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	public class RequestHandler implements Runnable{

		Socket client;

		public RequestHandler(Socket client){
			this.client = client;
		}

		public void run(){
			try {
				// Create input/output streams to TwoPC coordinator
				ObjectInputStream twoPcReader = new ObjectInputStream(client.getInputStream());
				ObjectOutputStream twoPcWriter = new ObjectOutputStream(client.getOutputStream());

				// Read message from 2pc coordinator
				WriteObject request = (WriteObject)twoPcReader.readObject();
				long txn = request.getTransactionId();

				// First, get the locks


				//                  Implement here


				//////////////////////////////////////////////////////////////////////////////////

				// Read config.properties file. Connect to Paxos Acceptors(Servers) [2 in our case]
				Properties properties = new Properties();
				ClassLoader classloader = Thread.currentThread().getContextClassLoader();
				InputStream is = classloader.getResourceAsStream("config.properties");
				try {
					properties.load(is);
				} catch (IOException e) {	
					e.printStackTrace();
					return;
				}

				// Connect and get sockets ready for communication
				Socket AcceptorOne = new Socket("127.0.0.1", 5001);
				Socket AcceptorTwo = new Socket(properties.getProperty("dataCenter2Ip"), 5002);
				Socket AcceptorThree = new Socket(properties.getProperty("dataCenter3Ip"), 5003);

				ObjectOutputStream acceptorOneWriter = new ObjectOutputStream(AcceptorOne.getOutputStream());
				ObjectOutputStream acceptorTwoWriter = new ObjectOutputStream(AcceptorTwo.getOutputStream());
				ObjectOutputStream acceptorThreeWriter = new ObjectOutputStream(AcceptorThree.getOutputStream());

				ObjectInputStream acceptorOneReader = new ObjectInputStream(AcceptorOne.getInputStream());
				ObjectInputStream acceptorTwoReader = new ObjectInputStream(AcceptorTwo.getInputStream());
				ObjectInputStream acceptorThreeReader = new ObjectInputStream(AcceptorThree.getInputStream());


				///////////////////////////////////////////////////////////////////////////////////

				for(Message m: request.getMessages()) {
					String key = m.getKey();
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

				twoPcWriter.writeObject(new Message(Command.ACCEPT));
				twoPcWriter.flush();
				Message response = (Message)twoPcReader.readObject();
				if(response.getCommand() == Command.ACCEPT){
					for(Message m: request.getMessages()){
						synchronized (locks) {
							locks.put(m.getKey(), txn);

							System.out.println("Key to be written is: " + m.getKey());
							System.out.println("Message is: " + m);
							jedis.hmset(m.getKey(), m.getValues());
						}
					}
					for(Message m: request.getMessages()){
						synchronized (locks){
							locks.remove(m.getKey());
							locks.notifyAll();
						}
					}
					twoPcWriter.writeObject(new Message(Command.SUCCESS));
					twoPcWriter.flush();
				}
				else{
					twoPcWriter.writeObject(new Message(Command.FAILURE));
					twoPcWriter.flush();
				}


			} catch (Exception e){
				e.printStackTrace();
			}

		}
	}
}
