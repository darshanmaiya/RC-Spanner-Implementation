package edu.ucsb.cs274.paxos;
import java.util.*;

import redis.clients.jedis.Jedis;

import java.io.*;
import java.net.*;

public class Server implements Runnable{

	// Members
	private int							id;
	private List<ServerInfo>			acceptors;
	private int							port;
	private int 						minProposal;
	private Jedis 						jedis1;
	private Jedis 						jedis2;
	private Jedis 						jedis3;
	private int							twoPcPort;
	private Socket						leaderSocket;

	// Getters and Setters
	public int getId(){ return this.id; }
	public List<ServerInfo> getParticipants(){ return this.acceptors; }
	public int getPort() { return this.port; };
	public int getMinProposal() { return this.minProposal; }

	public Server(int id){
		this.id = id;
		this.minProposal = 0;
		this.acceptors = new ArrayList<ServerInfo>();
	}

	Server(Socket leaderSocket, int id){
		this.id = id;
		this.minProposal = 0;
		this.acceptors = new ArrayList<ServerInfo>();
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

			// Connect to all Redis Servers
			jedis1 = new Jedis(properties.getProperty("redisServer1"), Integer.valueOf(properties.getProperty("redisPort1")));
			jedis2 = new Jedis(properties.getProperty("redisServer2"), Integer.valueOf(properties.getProperty("redisPort2")));
			jedis3 = new Jedis(properties.getProperty("redisServer3"), Integer.valueOf(properties.getProperty("redisPort3")));
			
			jedis1.connect();
			jedis2.connect();
			jedis3.connect();
			
			// Get 2PC-Port
			this.twoPcPort = Integer.valueOf(properties.getProperty("twoPcPort"));
		} 
		catch(Exception e){
			e.printStackTrace();
		}

	}

	public void finalize () {
		jedis1.disconnect();
		jedis2.disconnect();
		jedis3.disconnect();
	}

	// Setup/Manage ports, start message flow
	public void run () {

		this.initialize();
		
		try{
			WriteObject leaderMessage;

			// Once connected to LEADER, listen for Paxos message from the LEADER
			ObjectOutputStream leaderWriter = new ObjectOutputStream(leaderSocket.getOutputStream());
			ObjectInputStream leaderReader = new ObjectInputStream(leaderSocket.getInputStream());

			Message twoPcReply;

			while (true)
			{
				leaderMessage = (WriteObject) leaderReader.readObject();
				System.out.println("Read from LEADER:\n");

				// If PaxosPrepareRPC
				if (leaderMessage.getCommand() == Command.PREPARE) {
					System.out.println("Prepare");

					Socket twoPc = new Socket("127.0.0.1", this.twoPcPort);
					ObjectOutputStream twoPcWrite = new ObjectOutputStream(twoPc.getOutputStream());
					ObjectInputStream twoPcRead = new ObjectInputStream(twoPc.getInputStream());

					// Get maxRound
					int receivedMaxRound = leaderMessage.getMaxVal();

					// Check with LatestAcceptedProposal if received proposal is latest or not
					// If received proposal is latest
					if (receivedMaxRound > this.minProposal)
					{	
						this.minProposal = receivedMaxRound;

						// Send VALUE to 2PC module, check for its response(ready to commit or not), send PROMISE or NACK accordingly						

						// Send received WriteObject to 2PC co-ordinator for confirmation
						twoPcWrite.writeObject(leaderMessage);
						twoPcWrite.flush();

						// Wait till receive 'COMMIT' 
						twoPcReply = (Message)twoPcRead.readObject();

						if (twoPcReply.getCommand() == Command.COMMIT)
						{	
							leaderWriter.writeObject(new WriteObject(Command.PROMISE, leaderMessage.getTransactionId()));
							leaderWriter.flush();

							leaderMessage = (WriteObject)leaderReader.readObject();

							// If PaxosAcceptRPC
							if (leaderMessage.getCommand() == Command.ACCEPT) {
								System.out.println("Accept");
								// Get maxRound
								receivedMaxRound = leaderMessage.getMaxVal();

								if (receivedMaxRound >= this.minProposal){
									this.minProposal = receivedMaxRound;

									/*	for (Message m : leaderMessage.getMessages())
									{
										jedis.hmset(m.getKey(), m.getValues());
									}
									 */
									// Give a go-ahead to 2PC module to commit the value. Once acknowledgement received, send COMMIT to LEADER
									twoPcWrite.writeObject(new Message(Command.ACCEPT));
									twoPcWrite.flush();

									// Check for SUCCESS for 2PC Co-ordinator
									twoPcReply = (Message)twoPcRead.readObject();

									if (twoPcReply.getCommand() == Command.SUCCESS){
										leaderWriter.writeObject(new WriteObject(Command.SUCCESS, leaderMessage.getTransactionId()));
										leaderWriter.flush();
									}
									else
									{
										leaderWriter.writeObject(new WriteObject(Command.FAILURE, leaderMessage.getTransactionId()));
										leaderWriter.flush();
									}
								}
							}// LEADER sent ABORT
							else{
								twoPcWrite.writeObject(new Message(Command.ABORT));
								twoPcWrite.flush();
							}
						}
						// Else send NACK
						else{
							// Send NACK
							leaderWriter.writeObject(new WriteObject(Command.NACK, leaderMessage.getTransactionId()));
							leaderWriter.flush();
						}
					}
					// Received proposal is not latest
					else{

						// Send NACK
						leaderWriter.writeObject(new WriteObject(Command.NACK, leaderMessage.getTransactionId()));
						leaderWriter.flush();
					}
					twoPc.close();
				}

				if (leaderMessage.getCommand()  == Command.READ) {

					String key = leaderMessage.getMessages().get(0).getKey();
					Set<String> fields = leaderMessage.getMessages().get(0).getFields();
					HashMap<String, String> result = new HashMap<>();
					
					char keyId = key.charAt(key.length()-1);
					int shardNo = (Integer.valueOf(keyId))%3 + 1;
					
					// Get values
					if (fields == null) {
						if (shardNo == 1){
							result.putAll(jedis1.hgetAll(key));
						}
						if (shardNo == 2){
							result.putAll(jedis2.hgetAll(key));
						}
						if (shardNo == 3){
							result.putAll(jedis3.hgetAll(key));
						}
					} else {

						String[] fieldArray =
								(String[]) leaderMessage.getMessages().get(0).getFields().toArray(new String[fields.size()]);
						
						List<String> values;
						
						if (shardNo == 1){
							values = jedis1.hmget(key, fieldArray);
						}else if (shardNo == 2){
							values = jedis2.hmget(key, fieldArray);
						}
						else{
							values = jedis3.hmget(key, fieldArray);
						}
	
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
