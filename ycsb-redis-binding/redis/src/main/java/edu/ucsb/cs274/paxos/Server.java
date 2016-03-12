package edu.ucsb.cs274.paxos;
import java.util.*;

import redis.clients.jedis.Jedis;

import java.io.*;
import java.net.*;

public class Server {

	// Members
	private int							id;
	private List<ServerInfo>			acceptors;
	private int							port;
	private ServerSocket				serverSocket;
	private int 						minProposal;
	private String 						redisServer;
	private int							redisPort;
	private Jedis 						jedis;

	// Getters and Setters
	public int getId(){ return this.id; }
	public List<ServerInfo> getParticipants(){ return this.acceptors; }
	public int getPort() { return this.port; };
	public int getMinProposal() { return this.minProposal; }

	// Always use this constructor to initialize a new PaxosServer instance
	public Server(int id){
		this.id = id;
		this.minProposal = 0;
		this.acceptors = new ArrayList<ServerInfo>();
	}

	// Initialize all participants
	public void initialize(){

		// Open config file and read the participant IP/Port
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

			int totalParticipants = Integer.valueOf(properties.getProperty("totalParticipants"));
			for(int i=0; i<totalParticipants; i++) {
				String propIpAddress = properties.getProperty("ipAddress" + i);
				int propPort = Integer.valueOf(properties.getProperty("port" + i));
				if(i == this.id) {
					this.port = propPort;
					this.redisServer = properties.getProperty("redisServer" + i);
					this.redisPort = Integer.valueOf(properties.getProperty("redisPort" + i));
				} else {
					ServerInfo server = new ServerInfo(i, propIpAddress, propPort);
					
					this.acceptors.add(server);
				}
			}
			
			// Connect to redis server
			jedis = new Jedis(this.redisServer, this.redisPort);
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
	public void start () {
		try{
			WriteObject leaderMessage;
			this.serverSocket = new ServerSocket(this.port);
			Socket leaderSocket = this.serverSocket.accept();

			// Once connected to LEADER, listen for Paxos message from the LEADER
			ObjectOutputStream leaderWriter = new ObjectOutputStream(leaderSocket.getOutputStream());
			ObjectInputStream leaderReader = new ObjectInputStream(leaderSocket.getInputStream());

			while (true)
			{
				// First connect to corresponding 2PC module
				leaderMessage = (WriteObject) leaderReader.readObject();
				System.out.println("Read from LEADER:\n" + leaderMessage);

				// If PaxosPrepareRPC
				if (leaderMessage.getCommand() == Command.PREPARE) {
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
						// Else send NACK
					}
					// Received proposal is not latest
					else{
						// Send NACK
						leaderWriter.writeObject(new WriteObject(Command.NACK, leaderMessage.getTransactionId()));
						leaderWriter.flush();
					}
				}

				// If PaxosAcceptRPC
				if (leaderMessage.getCommand() == Command.ACCEPT) {

					// Get maxRound
					int receivedMaxRound = leaderMessage.getMaxVal();
					
					if (receivedMaxRound >= this.minProposal){
						this.minProposal = receivedMaxRound;
					
						for (Message m : leaderMessage.getMessages())
						{
							jedis.hmset(m.getKey(), m.getValues());
						}
						// Give a go-ahead to 2PC module to commit the value. Once acknowledgement received, send COMMIT to LEADER
						leaderWriter.writeObject(new WriteObject(Command.SUCCESS, leaderMessage.getTransactionId()));
						leaderWriter.flush();
					}
				}
				// If ABORT
				if (leaderMessage.getCommand()  == Command.ABORT) {
					// Send ABORT message to 2PC module
				}
				
				if (leaderMessage.getCommand()  == Command.READ) {

					String key = leaderMessage.getMessages().get(0).getKey();
					Set<String> fields = leaderMessage.getMessages().get(0).getFields();
				    HashMap<String, String> result = new HashMap<>();
					// Get values
				/*	if (fields == null) {
				      result.putAll(jedis.hgetAll(leaderMessage.getMessages().get(0).getKey()));
				    } else {
				      String[] fieldArray =
				          (String[]) leaderMessage.getMessages().get(0).getFields().toArray(new String[fields.size()]);
				      List<String> values = jedis.hmget(key, fieldArray);

				      Iterator<String> fieldIterator = fields.iterator();
				      Iterator<String> valueIterator = values.iterator();

				      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
				        result.put(fieldIterator.next(),
				            valueIterator.next());
				      }
				    }
				 */
					// Give a go-ahead to 2PC module to commit the value. Once acknowledgement received, send COMMIT to LEADER
					leaderWriter.writeObject(new WriteObject(Command.SUCCESS, leaderMessage.getTransactionId()));
					leaderWriter.flush();
				}
			}
		}

		catch (Exception e){
			e.printStackTrace();
		}
	}
}
