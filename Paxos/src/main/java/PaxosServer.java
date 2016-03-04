import java.util.*;
import java.io.*;
import java.net.*;

public class PaxosServer {

	// Members
	private String						state;				// State: LEADER, ACCEPTOR
	private int 						maxRound; 
	private int							id;
	private List<Server>				participants;
	private Server 						leader;
	private String 						ipAddress;
	private int							port;
	private ServerSocket				serverSocket;
	private Socket 						redisYcsbSocket;
	private Server 						redisYcsbServer;
	private int 						minProposal;
	//	private List<Socket>				acceptorSocket;
	//	private List<PrintWriter>			acceptorWriter;
	//	private List<BufferedReader>		acceptorReader;
	//	private int 						numPaxosServers;
	//	private HashMap<Integer, Socket>	clientSockets;

	// Getters and Setters
	public String getState(){ return this.state; }
	public int getMaxRound(){ return this.maxRound; }
	public int getId(){ return this.id; }
	public void setLeader(Server leader) {this.leader = leader;}
	public List<Server> getParticipants(){ return this.participants; }
	public int getPort() { return this.port; };
	public Socket getRedisYcsbSocket() {return this.redisYcsbSocket; }
	public int getMinProposal() { return this.minProposal; }

	// Constructors
	public PaxosServer(){}

	// Always use this constructor to initialize a new PaxosServer instance
	public PaxosServer(int id){
		this.id = id;

		if (this.id == 1){
			this.state = "LEADER";
		}else { this.state = "ACCEPTOR";}

		this.maxRound = 0;
		this.minProposal = 0;
		this.participants = new ArrayList<Server>();
	}

	// Initialize all participants
	public void initialize(){

		// Open config file and read the participant IP/Port
		try{
			BufferedReader fileReader;
			fileReader = new BufferedReader(new FileReader("serverConfig.txt"));
			String line = null;
			String[] serverInfo; 

			while ((line = fileReader.readLine()) != null){
				serverInfo = line.split(" ");

				// Not this server
				if (Integer.parseInt(serverInfo[0]) != this.id){			
					Server newServer = new Server(Integer.parseInt(serverInfo[0]), serverInfo[1], Integer.parseInt(serverInfo[2]));

					// If parsed Server is a LEADER
					if (Integer.parseInt(serverInfo[0]) == 1){
						this.leader = newServer;
					}
					// If parsed Server is a Redis YCSB server
					else if (Integer.parseInt(serverInfo[0]) == 0){
						this.redisYcsbServer = newServer;
					}
					else{
						this.participants.add(newServer);
					}
				}
				else{
					this.ipAddress = serverInfo[1];
					this.port = Integer.parseInt(serverInfo[2]);
				}
			}
			fileReader.close();
		} 
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// Setup/Manage ports, start message flow
	public void start(){
		// If: This Server itself is the LEADER
		if (this.state == "LEADER"){
			try {
				// Connect to redis YCSB server
				this.redisYcsbSocket = new Socket(redisYcsbServer.getIpAddress(), redisYcsbServer.getPort());

				// Connect to all participants
				for (Server acceptor : this.participants){

					Socket newSocket = new Socket(acceptor.getIpAddress(), acceptor.getPort());
					PrintWriter acceptorWrite = new PrintWriter(new OutputStreamWriter(newSocket.getOutputStream()));
					BufferedReader acceptorRead = new BufferedReader(new InputStreamReader(newSocket.getInputStream()));
					acceptor.setAcceptorSocket(newSocket);
					acceptor.setAcceptorWriter(acceptorWrite);
					acceptor.setAcceptorReader(acceptorRead);
				}

				String ycsbMessage;
				PrintWriter ycsbWriter = new PrintWriter(new OutputStreamWriter(redisYcsbSocket.getOutputStream()));
				BufferedReader ycsbReader = new BufferedReader(new InputStreamReader(redisYcsbSocket.getInputStream()));
				String acceptorMessage;
				int numPromises = 0;

				// Wait for message from Redis YCSB server, send it to Paxos Acceptors 
				while (true){

					String prepareRPC;
					String acceptRPC;

					// When there is a message from YCSB server, initiate Paxos.
					while ((ycsbMessage = ycsbReader.readLine()) != null){

						numPromises = 0;
						int majority = (this.getParticipants().size() + 1)/2;

						System.out.println("Read from YcSB : " + ycsbMessage);

						// Broadcast PrepareRPC to all Acceptors
						prepareRPC = PaxosPrepareRPC(ycsbMessage);

						// Phase 1
						for (Server acceptor : this.participants){

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
							for (Server acceptor : this.participants){

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

							for (Server acceptor : this.participants){

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
		// Else: Server is an ACCEPTOR, open port for LEADER to connect
		else{
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

						// Get Server ID
						String serverIdString = extract[1].substring(extract[1].indexOf("=")+1);
						int serverID = Integer.parseInt(serverIdString);

						// Get values
						String value = extract[2].substring(extract[2].indexOf("=")+1);

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
						String maxRoundString = extract[0].substring(extract[0].indexOf("=")+1);
						int receivedMaxRound = Integer.parseInt(maxRoundString);

						// Get Server ID
						String serverIdString = extract[1].substring(extract[1].indexOf("=")+1);
						int serverID = Integer.parseInt(serverIdString);

						// Get values
						String value = extract[2].substring(extract[2].indexOf("=")+1);

						if (receivedMaxRound >= this.minProposal){
							this.minProposal = receivedMaxRound;
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

	// Generate Paxos PrepareRPC
	private String PaxosPrepareRPC(String ycsbMessage){
		// Prepare message will be of the form, "PREPARE=123 ID=1 COMMIT=X:2 Y:3 Z:4"
		// Proposal no = "(maxRound + 1)" + "Server ID"
		this.maxRound++;
		String proposeMessage = "PREPARE=" + this.maxRound + " ID=" + this.id + " " + ycsbMessage;
		return proposeMessage;
	}

	// Generate Paxos AcceptRPC
	private String PaxosAcceptRPC(String ycsbMessage){
		// Accept message will be of the form, "ACCEPT=1 ID=1 COMMIT=X:2 Y:3 Z:4"
		String acceptMessage = "ACCEPT=" + this.maxRound + " ID=" + this.id + " " + ycsbMessage;
		return acceptMessage;
	}

	// Generate Paxos AbortRPC
	private String PaxosAbortRPC(String ycsbMessage){
		// Abort message will be of the form, "ABORT=1 ID=1 COMMIT=X:2 Y:3 Z:4"
		String abortMessage = "ABORT=" + this.maxRound + "ID=" + this.id + " " + ycsbMessage;
		return abortMessage;
	}

	public static void main(String args[]){
		PaxosServer p1 = new PaxosServer(1);
		p1.initialize();
		p1.start();
	}
}