package edu.ucsb.cs274.paxos;

import java.util.*;
import java.io.*;
import java.net.*;

public class Client implements Runnable{

	// Members
	private int 							maxRound;
	private List<ServerInfo>				acceptors;
	private int								port;
	private ServerSocket					redisYcsbClientSocket;
	private	Socket 							redisClientSocket;

//	private HashMap<Long, ClientHandler>	transactions;

	// Getters and Setters
	public int getMaxRound(){ return this.maxRound; }
	public List<ServerInfo> getAcceptors(){ return this.acceptors; }
	public int getPort() { return this.port; };

	public Client() {
		this.maxRound = 0;
		this.acceptors = new ArrayList<ServerInfo>();
	}

	Client(Socket redisClientSocket){
		this.maxRound = 0;
		this.acceptors = new ArrayList<ServerInfo>();
	    try {
	      this.redisClientSocket = redisClientSocket;
	    }
	    catch (Exception e){
	      e.printStackTrace();
	    }
	  }

	// Setup/Manage ports, start message flow
	public void run() {
		
		// First, initialize
		try {
	        
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

			// Connect to all acceptors
			for (ServerInfo acceptor : this.acceptors) {

				Socket newSocket = new Socket(acceptor.getIpAddress(), acceptor.getPort());
				ObjectOutputStream acceptorWrite = new ObjectOutputStream(new BufferedOutputStream(newSocket.getOutputStream()));
				ObjectInputStream acceptorRead = new ObjectInputStream(new BufferedInputStream(newSocket.getInputStream()));
				acceptor.setAcceptorSocket(newSocket);
				acceptor.setAcceptorWriter(acceptorWrite);
				acceptor.setAcceptorReader(acceptorRead);
			}

			WriteObject ycsbMessage;
			ObjectOutputStream ycsbWriter = new ObjectOutputStream(new BufferedOutputStream(redisClientSocket.getOutputStream()));
			ObjectInputStream ycsbReader = new ObjectInputStream(new BufferedInputStream(redisClientSocket.getInputStream()));
			WriteObject acceptorMessage = null;
			int numPromises = 0;

			// Wait for message from Redis YCSB client, send it to Paxos Acceptors 
			while (true) {

				WriteObject prepareRPC;
				WriteObject acceptRPC;
				WriteObject abortRPC;

				// When there is a message from YCSB server, initiate Paxos.
				try {
					ycsbMessage = (WriteObject)ycsbReader.readObject();
				//	System.out.println("YCSB command: " + " Transaction id: " + ycsbMessage.getTransactionId() + " Command: " + ycsbMessage.getCommand());
				} catch (EOFException eof) {
					continue;
				}
				
				Command command = ycsbMessage.getCommand();
			//	System.out.println("Message from YCSB Client:\n");
				int majority = (this.getAcceptors().size()/2) + 1;
				
				if (command == Command.COMMIT) {
					numPromises = 0;

				//	System.out.println("Received message: "  + ycsbMessage.getMessages());
					// Broadcast PrepareRPC to all Acceptors
					prepareRPC = PaxosPrepareRPC(ycsbMessage);

					// Phase 1
					for (ServerInfo acceptor : this.acceptors) {

						// Check if Acceptor socket is still open
						if (!acceptor.getAcceptorSocket().isClosed()) {

							// Send Paxos prepare message to Acceptors
							acceptor.getAcceptorWriter().writeObject(prepareRPC);
							acceptor.getAcceptorWriter().flush();
							
							// Read reply message from Acceptor
							acceptorMessage = (WriteObject)acceptor.getAcceptorReader().readObject();

							// Check if PROMISE or NACK
							if (acceptorMessage.getCommand() == Command.PROMISE) {
								numPromises++;
								acceptor.setAcceptedPrepare(true);
							}
						}
					}

					// Testing
				//	System.out.println("Number of Promises: " + numPromises);

					// Check if Commit quorum achieved
					if (numPromises >= majority){

						int commitAccept = 0;

						// Create acceptRPC
						acceptRPC = PaxosAcceptRPC(ycsbMessage);
						
						// Phase 2	
						for (ServerInfo acceptor : this.acceptors){

							// Check if Acceptor socket is still open
							if (!acceptor.getAcceptorSocket().isClosed()){

								// Send Paxos ACCEPT message to servers that responded PROMISE
								if (acceptor.getAcceptedPrepare())
								{
									acceptor.setAcceptedPrepare(false);
									acceptor.getAcceptorWriter().writeObject(acceptRPC);
									acceptor.getAcceptorWriter().flush();

									// Read reply message from Acceptor
									acceptorMessage = (WriteObject)acceptor.getAcceptorReader().readObject();

									if (acceptorMessage.getCommand() == Command.SUCCESS){
									//	System.out.println("Acceptance message: \n" + acceptorMessage);
										commitAccept++;
									}
								}
							}
						}
						
						if (commitAccept >= majority){
							ycsbWriter.writeObject(new WriteObject(Command.SUCCESS, ycsbMessage.getTransactionId()));
							ycsbWriter.flush();
						}
					}
					else
					{
						// Send ABORT to servers that responded with PROMISE
						abortRPC = PaxosAbortRPC(ycsbMessage.getTransactionId());

						for (ServerInfo acceptor : this.acceptors) {

							// Check if Acceptor socket is still open
							if (!acceptor.getAcceptorSocket().isClosed()) {

								// Send ABORT message to servers that responded PROMISE
								if (acceptor.getAcceptedPrepare())
								{
									acceptor.setAcceptedPrepare(false);
									acceptor.getAcceptorWriter().writeObject(abortRPC);
									acceptor.getAcceptorWriter().flush();
								}
							}
						}

						ycsbWriter.writeObject(new WriteObject(Command.ABORT, ycsbMessage.getTransactionId()));
						ycsbWriter.flush();
					}
				} else if (command == Command.READ) {
					int numSuccess = 0;
					// Phase 1
					for (ServerInfo acceptor : this.acceptors) {

						// Check if Acceptor socket is still open
						if (!acceptor.getAcceptorSocket().isClosed()){

							// Send Read request to Acceptors
							acceptor.getAcceptorWriter().writeObject(ycsbMessage);
							acceptor.getAcceptorWriter().flush();
							
							// Read reply message from Acceptor
							acceptorMessage = (WriteObject)acceptor.getAcceptorReader().readObject();

							// Check if SUCCESS
							if (acceptorMessage.getCommand() == Command.SUCCESS)
							{
								numSuccess++;
							}
						}
					}
					

					if(numSuccess >= majority) {
						ycsbWriter.writeObject(acceptorMessage);
						ycsbWriter.flush();
					}
				}
			}
		}catch (Exception e){
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

	// Generate Paxos AcceptRPC
	private WriteObject PaxosAcceptRPC(WriteObject ycsbMessage) {
		// Accept message will be of the form, "ACCEPT=1 COMMIT=X:2 Y:3 Z:4"
		
		WriteObject acceptRPC = new WriteObject(Command.ACCEPT, ycsbMessage.getTransactionId(), ycsbMessage.getMessages(), this.maxRound);
		
		return acceptRPC;
	}

	// Generate Paxos AbortRPC
	private WriteObject PaxosAbortRPC(long transactionId) {
		// Abort message will be of the form, "ABORT=1 COMMIT=X:2 Y:3 Z:4"
		
		WriteObject abortRPC = new WriteObject(Command.ABORT, transactionId);
		
		return abortRPC;
	}

	/*public static void main(String args[]) {
		Runnable myRunnable1 = new Runnable(){

		     public void run(){
		        System.out.println("Starting paxos server 1");

				Server p1 = new Server(1);
				p1.initialize();
			//	p1.start();
		     }
		};
		
		Thread thread1 = new Thread(myRunnable1);
		thread1.start();
		
//		Runnable myRunnable2 = new Runnable(){
//
//		     public void run(){
//		        System.out.println("Starting paxos server 2");
//
//				Server p2 = new Server(2);
//				p2.initialize();
//				p2.start();
//		     }
//		};
//
//		Thread thread2 = new Thread(myRunnable2);
//		thread2.start();
//
//		Runnable myRunnable3 = new Runnable(){
//
//		     public void run(){
//		        System.out.println("Starting paxos server 3");
//
//				Server p3 = new Server(3);
//				p3.initialize();
//				p3.start();
//		     }
//		};
//
//		Thread thread3 = new Thread(myRunnable3);
//		thread3.start();
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Starting paxos client");
		
		Client c = new Client();
	//	c.initialize();
		//c.start();
	}*/
}
