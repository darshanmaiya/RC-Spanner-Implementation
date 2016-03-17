package edu.ucsb.cs274.spanner;
import java.util.*;
import edu.ucsb.cs274.paxos.WriteObject;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.Command;

import redis.clients.jedis.Jedis;

import java.io.*;
import java.net.*;

public class Server implements Runnable{

	// Members
	private int							id;
	private int							port;
	private int 						minProposal;
	private Jedis 						jedis;
	private Socket						leaderSocket;

	// Getters and Setters
	public int getId(){ return this.id; }
	public int getPort() { return this.port; };
	public int getMinProposal() { return this.minProposal; }

	public Server(int id){
		this.id = id;
		this.minProposal = 0;
	}

	Server(Socket leaderSocket, int id){
		this.id = id;
		this.minProposal = 0;
		try {
			this.leaderSocket = leaderSocket;
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	// Initialize all participants
	public void initialize(){
		// Open config file, read 2PC port details and connect to corresponding redis server
		try{			
			Properties properties = new Properties();
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			InputStream is = classloader.getResourceAsStream("config.properties");
			try {
				properties.load(is);
			} catch (IOException e) {

				e.printStackTrace();
				return;
			}

			// Connect to corresponding Redis Servers
			jedis = new Jedis(properties.getProperty("redisServer" + String.valueOf(this.id)), Integer.valueOf(properties.getProperty("redisPort" + String.valueOf(this.id))));
			jedis.connect();
		} 
		catch(Exception e){
			e.printStackTrace();
		}

	}

	public void finalize () {
		jedis.disconnect();
	}

	// Setup/Manage ports, start message flow
	public void run () {

		this.initialize();

		try{
			WriteObject leaderMessage;

			// Once connected to LEADER, listen for Paxos message from the LEADER
			ObjectOutputStream leaderWriter = new ObjectOutputStream(leaderSocket.getOutputStream());
			ObjectInputStream leaderReader = new ObjectInputStream(leaderSocket.getInputStream());

			while (true)
			{
				leaderMessage = (WriteObject) leaderReader.readObject();
				System.out.println("Read from LEADER");

				// If PaxosPrepareRPC
				if (leaderMessage.getCommand() == Command.PREPARE) {
					System.out.println("Prepare");

					// Get maxRound
					int receivedMaxRound = leaderMessage.getMaxVal();

					// Check with LatestAcceptedProposal if received proposal is latest or not
					// If received proposal is latest
					if (receivedMaxRound > this.minProposal)
					{	
						this.minProposal = receivedMaxRound;

						// Send VALUE to 2PC module, check for its response(ready to commit or not), send PROMISE or NACK accordingly						
						leaderWriter.writeObject(new WriteObject(Command.PROMISE, leaderMessage.getTransactionId()));
						leaderWriter.flush();	
					}
					// Received proposal is not latest
					else{
						// Send NACK
						leaderWriter.writeObject(new WriteObject(Command.NACK, leaderMessage.getTransactionId()));
						leaderWriter.flush();
					}
				}

				if (leaderMessage.getCommand() == Command.READ) {

					String key = leaderMessage.getMessages().get(0).getKey();
					Set<String> fields = leaderMessage.getMessages().get(0).getFields();
					HashMap<String, String> result = new HashMap<>();
					
					char keyId = key.charAt(key.length()-1);
					int shardNo = (Integer.valueOf(keyId))%3 + 1;
					
					// Get values
					if (fields == null) {
							result.putAll(jedis.hgetAll(key));
					} else {

						String[] fieldArray =
								(String[]) leaderMessage.getMessages().get(0).getFields().toArray(new String[fields.size()]);
						
						List<String> values;
						
						values = jedis.hmget(key, fieldArray);
						
						Iterator<String> fieldIterator = fields.iterator();
						Iterator<String> valueIterator = values.iterator();

						while (fieldIterator.hasNext() && valueIterator.hasNext()) {
							result.put(fieldIterator.next(),
									valueIterator.next());
						}
					}

					List<Message> messageList = new ArrayList<Message>();
					messageList.add(new Message (Command.SUCCESS, key, null, result));

					// Send the READ value to the leader
					leaderWriter.writeObject(new WriteObject(Command.SUCCESS, leaderMessage.getTransactionId(), messageList));
					leaderWriter.flush();
				}
			}
		}

		catch (Exception e){
			e.printStackTrace();
		}
	}
}
