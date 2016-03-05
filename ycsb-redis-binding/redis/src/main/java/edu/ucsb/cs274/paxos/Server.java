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
			String leaderMessage;
			this.serverSocket = new ServerSocket(this.port);
			Socket leaderSocket = this.serverSocket.accept();

			// Once connected to LEADER, listen for Paxos message from the LEADER
			PrintWriter leaderWriter = new PrintWriter(new OutputStreamWriter(leaderSocket.getOutputStream()));
			BufferedReader leaderReader = new BufferedReader(new InputStreamReader(leaderSocket.getInputStream()));

			while (true)
			{
				// First connect to corresponding 2PC module
				leaderMessage = leaderReader.readLine();
				System.out.println("Read from LEADER : " + leaderMessage);

				// If PaxosPrepareRPC
				if (leaderMessage.contains("PREPARE")){
					String[] extract = leaderMessage.split("\\s+");

					// Get maxRound
					String maxRoundString = extract[0].substring(extract[0].indexOf("=")+1);
					int receivedMaxRound = Integer.parseInt(maxRoundString);
					
					// Get values
					//String value = extract[1].substring(extract[1].indexOf("=")+1);

					// Check with LatestAcceptedProposal if received proposal is latest or not
					// If received proposal is latest
					if (receivedMaxRound > this.minProposal)
					{	
						this.minProposal = receivedMaxRound;

						// Send VALUE to 2PC module, check for its response(ready to commit or not), send PROMISE or NACK accordingly
						leaderWriter.println("PROMISE");
						leaderWriter.flush();
						// Else send NACK
					}
					// Received proposal is not latest
					else{
						// Send NACK
						leaderWriter.println("NACK");
						leaderWriter.flush();
					}
				}

				// If PaxosAcceptRPC
				if (leaderMessage.contains("ACCEPT")){

					String[] extract = leaderMessage.split("\\s+");

					// Get maxRound
					String maxRoundString = extract[0].substring(extract[0].indexOf("=") + 1);
					int receivedMaxRound = Integer.parseInt(maxRoundString);

					// Get values
					String value = leaderMessage.substring(leaderMessage.indexOf("COMMIT") + 7);

					if (receivedMaxRound >= this.minProposal){
						this.minProposal = receivedMaxRound;
						// "COMMIT=X:2 Y:3 Z:4"
						jedis.hset("ipAddress" + this.id, "", value);
						// Give a go-ahead to 2PC module to commit the value. Once acknowledgement received, send COMMIT to LEADER
						leaderWriter.println("COMMIT=SUCCESS");
						leaderWriter.flush();
					}
				}
				// If ABORT
				if (leaderMessage.contains("ABORT")){
					// Send ABORT message to 2PC module
				}
			}
		}

		catch (Exception e){
			e.printStackTrace();
		}
	}
}
