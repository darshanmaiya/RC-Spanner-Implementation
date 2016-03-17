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
				(new Thread(paxosLeader.new RequestHandler(client, 1))).start();
				(new Thread(paxosLeader.new RequestHandler(client, 2))).start();
				(new Thread(paxosLeader.new RequestHandler(client, 3))).start();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	public class RequestHandler implements Runnable{
		int id;
		Socket client;
		int maxRound;
		Socket acceptorOne;
		Socket acceptorTwo;

		public RequestHandler(Socket client, int id){
			this.id = id;
			this.maxRound = 0;
			this.client = client;
		}

		public void run(){
			try {

				// Initialize

				// Create input/output streams to TwoPC coordinator
				ObjectInputStream twoPcReader = new ObjectInputStream(client.getInputStream());
				ObjectOutputStream twoPcWriter = new ObjectOutputStream(client.getOutputStream());

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

				// Connect to corresponding acceptors
				if (1 == this.id){
					this.acceptorOne = new Socket(properties.getProperty("dataCenterIp2"), 5002);
					this.acceptorTwo = new Socket(properties.getProperty("dataCenterIp3"), 5003);
				}
				
				if (2 == this.id){
					this.acceptorOne = new Socket(properties.getProperty("dataCenterIp1"), 5001);
					this.acceptorTwo = new Socket(properties.getProperty("dataCenterIp3"), 5003);
				}
				
				if (3 == this.id){
					this.acceptorOne = new Socket(properties.getProperty("dataCenterIp1"), 5001);
					this.acceptorTwo = new Socket(properties.getProperty("dataCenterIp2"), 5002);
				}

				ObjectOutputStream acceptorOneWriter = new ObjectOutputStream(acceptorOne.getOutputStream());
				ObjectOutputStream acceptorTwoWriter = new ObjectOutputStream(acceptorTwo.getOutputStream());

				ObjectInputStream acceptorOneReader = new ObjectInputStream(acceptorOne.getInputStream());
				ObjectInputStream acceptorTwoReader = new ObjectInputStream(acceptorTwo.getInputStream());

				WriteObject prepareRPC;
				WriteObject acceptRPC;
				WriteObject abortRPC;
				WriteObject acceptorOneMessage;
				WriteObject acceptorTwoMessage;
				WriteObject twoPcMessage;
				
				while (true){

					// Read message from 2pc coordinator
					twoPcMessage = (WriteObject)twoPcReader.readObject();
					int majority = 2;
					int numPromises = 0;
					long txn = twoPcMessage.getTransactionId();

					///////////////////////// First, get the locks ///////////////////////////////////


					//                  Implement here


					//////////////////////////////////////////////////////////////////////////////////

					// Generate PaxosPrepare RPC
					prepareRPC = PaxosPrepareRPC(twoPcMessage);
					
					// Send prepare RPC to acceptor 1
					acceptorOneWriter.writeObject(prepareRPC);
					acceptorOneWriter.flush();
					
					acceptorOneMessage = (WriteObject)acceptorOneReader.readObject();
					if (acceptorOneMessage.getCommand() == Command.PROMISE)	
						numPromises++;
					
					// Send prepare RPC to acceptor 2
					acceptorTwoWriter.writeObject(prepareRPC);
					acceptorTwoWriter.flush();
					
					acceptorTwoMessage = (WriteObject)acceptorTwoReader.readObject();
					if (acceptorOneMessage.getCommand() == Command.PROMISE)	
						numPromises++;
					
					if (numPromises >= majority){
						// Send SUCCESS to 2PC
						twoPcWriter.writeObject(new WriteObject(Command.SUCCESS));
						twoPcWriter.flush();
					}
					else{
						twoPcWriter.writeObject(new WriteObject(Command.FAILURE));
						twoPcWriter.flush();
					}
					
				}
			} catch (Exception e){
				e.printStackTrace();
			}
		}

		// Generate Paxos PrepareRPC
		private WriteObject PaxosPrepareRPC(WriteObject ycsbMessage) {
			// Prepare message will be of the form, "PREPARE=123 COMMIT=X:2 Y:3 Z:4"
			// Proposal no = "(maxRound + 1)"
			this.maxRound++;

			WriteObject proposeObject = new WriteObject(Command.PREPARE, ycsbMessage.getTransactionId(), ycsbMessage.getMessages(), this.maxRound);

			return proposeObject;
		}
	}
}
