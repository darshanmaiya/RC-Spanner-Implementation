package edu.ucsb.cs274.paxos;

import java.util.*;
import java.io.*;
import java.net.*;

public class Client {

	// Members
	private int 						maxRound;
	private List<ServerInfo>			acceptors;
	private int							port;
	private ServerSocket				redisYcsbClientSocket;

	// Getters and Setters
	public int getMaxRound(){ return this.maxRound; }
	public List<ServerInfo> getAcceptors(){ return this.acceptors; }
	public int getPort() { return this.port; };

	// Always use this constructor to initialize a new PaxosServer instance
	public Client() {
		this.maxRound = 0;
		this.acceptors = new ArrayList<ServerInfo>();
	}

	// Initialize all Acceptors
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
				if(i == 0) {
					this.port = propPort;
				} else {
					ServerInfo server = new ServerInfo(i, propIpAddress, propPort);
					
					this.acceptors.add(server);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	// Setup/Manage ports, start message flow
	public void start() {
		
		try {
			// Open socket for Redis client to connect
			this.redisYcsbClientSocket = new ServerSocket(this.port);
            Socket redisClientSocket = this.redisYcsbClientSocket.accept();

			// Connect to all acceptors
			for (ServerInfo acceptor : this.acceptors) {

				Socket newSocket = new Socket(acceptor.getIpAddress(), acceptor.getPort());
				PrintWriter acceptorWrite = new PrintWriter(new OutputStreamWriter(newSocket.getOutputStream()));
				BufferedReader acceptorRead = new BufferedReader(new InputStreamReader(newSocket.getInputStream()));
				acceptor.setAcceptorSocket(newSocket);
				acceptor.setAcceptorWriter(acceptorWrite);
				acceptor.setAcceptorReader(acceptorRead);
			}

			String ycsbMessage;
			PrintWriter ycsbWriter = new PrintWriter(new OutputStreamWriter(redisClientSocket.getOutputStream()));
			BufferedReader ycsbReader = new BufferedReader(new InputStreamReader(redisClientSocket.getInputStream()));
			String acceptorMessage;
			int numPromises = 0;

			// Wait for message from Redis YCSB client, send it to Paxos Acceptors 
			while (true){

				String prepareRPC;
				String acceptRPC;

				// When there is a message from YCSB server, initiate Paxos.
				while ((ycsbMessage = ycsbReader.readLine()) != null){

					numPromises = 0;
					int majority = (this.getAcceptors().size()/2) + 1;

					System.out.println("Message from YCSB Client: " + ycsbMessage);

					// Broadcast PrepareRPC to all Acceptors
					prepareRPC = PaxosPrepareRPC(ycsbMessage);

					// Phase 1
					for (ServerInfo acceptor : this.acceptors){

						// Check if Acceptor socket is still open
						if (!acceptor.getAcceptorSocket().isClosed()){

							// Send Paxos prepare message to Acceptors
							acceptor.getAcceptorWriter().println(prepareRPC);
							acceptor.getAcceptorWriter().flush();
							
							// Read reply message from Acceptor
							acceptorMessage = acceptor.getAcceptorReader().readLine();

							// Check if PROMISE or NACK
							if (acceptorMessage.equals("PROMISE"))
							{
								numPromises++;
								acceptor.setAcceptedPrepare(true);
							}
						}
					}

					// Testing
					System.out.println("Number of Promises: " + numPromises);

					// Check if Commit quorum achieved
					if (numPromises >= majority){

						int commitAccept = 0;

						// Get acceptRPC
						acceptRPC = PaxosAcceptRPC(ycsbMessage);

						// Phase 2	
						for (ServerInfo acceptor : this.acceptors){

							// Check if Acceptor socket is still open
							if (!acceptor.getAcceptorSocket().isClosed()){

								// Send Paxos ACCEPT message to servers that responded PROMISE
								if (acceptor.getAcceptedPrepare())
								{
									acceptor.setAcceptedPrepare(false);
									acceptor.getAcceptorWriter().println(acceptRPC);
									acceptor.getAcceptorWriter().flush();

									// Read reply message from Acceptor
									acceptorMessage = acceptor.getAcceptorReader().readLine();

									if (acceptorMessage.contains("COMMIT=SUCCESS")){
										System.out.println(acceptorMessage);
										commitAccept++;
									}
								}
							}
						}
						
						if (commitAccept >= majority){
							ycsbWriter.println("COMMIT");
							ycsbWriter.flush();
						}
					}
					else
					{
						// Send ABORT to servers that responded with PROMISE
						String abortRPC = PaxosAbortRPC(ycsbMessage);

						for (ServerInfo acceptor : this.acceptors){

							// Check if Acceptor socket is still open
							if (!acceptor.getAcceptorSocket().isClosed()){

								// Send ABORT message to servers that responded PROMISE
								if (acceptor.getAcceptedPrepare())
								{
									acceptor.setAcceptedPrepare(false);
									acceptor.getAcceptorWriter().println(abortRPC);
									acceptor.getAcceptorWriter().flush();
								}
							}
						}

						ycsbWriter.println("ABORT");
						ycsbWriter.flush();
					}
				}
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	// Generate Paxos PrepareRPC
	private String PaxosPrepareRPC(String ycsbMessage){
		// Prepare message will be of the form, "PREPARE=123 COMMIT=X:2 Y:3 Z:4"
		// Proposal no = "(maxRound + 1)"
		this.maxRound++;
		String proposeMessage = "PREPARE=" + this.maxRound + " " + ycsbMessage;
		return proposeMessage;
	}

	// Generate Paxos AcceptRPC
	private String PaxosAcceptRPC(String ycsbMessage){
		// Accept message will be of the form, "ACCEPT=1 COMMIT=X:2 Y:3 Z:4"
		String acceptMessage = "ACCEPT=" + this.maxRound + " " + ycsbMessage;
		return acceptMessage;
	}

	// Generate Paxos AbortRPC
	private String PaxosAbortRPC(String ycsbMessage){
		// Abort message will be of the form, "ABORT=1 COMMIT=X:2 Y:3 Z:4"
		String abortMessage = "ABORT=" + this.maxRound + " " + ycsbMessage;
		return abortMessage;
	}

	public static void main(String args[]) {
		Runnable myRunnable1 = new Runnable(){

		     public void run(){
		        System.out.println("Starting paxos server 1");

				Server p1 = new Server(1);
				p1.initialize();
				p1.start();
		     }
		};
		
		Thread thread1 = new Thread(myRunnable1);
		thread1.start();
		
		Runnable myRunnable2 = new Runnable(){

		     public void run(){
		        System.out.println("Starting paxos server 2");

				Server p2 = new Server(2);
				p2.initialize();
				p2.start();
		     }
		};
		
		Thread thread2 = new Thread(myRunnable2);
		thread2.start();
		
		Runnable myRunnable3 = new Runnable(){

		     public void run(){
		        System.out.println("Starting paxos server 3");

				Server p3 = new Server(3);
				p3.initialize();
				p3.start();
		     }
		};
		
		Thread thread3 = new Thread(myRunnable3);
		thread3.start();
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Starting paxos client");
		
		Client c = new Client();
		c.initialize();
		c.start();
	}
}
