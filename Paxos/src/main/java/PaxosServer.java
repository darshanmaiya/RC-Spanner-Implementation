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

	// Setup/Manage ports
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

				// Wait for message from Redis YCSB server, send it to Paxos Acceptors 
				String ycsbMessage;
				PrintWriter ycsbWriter = new PrintWriter(new OutputStreamWriter(redisYcsbSocket.getOutputStream()));
				BufferedReader ycsbReader = new BufferedReader(new InputStreamReader(redisYcsbSocket.getInputStream()));
				String acceptorMessage;
				int numPromises = 0;
				
				while (true){
					
					// When there is a message from YCSB server, initiate Paxos.
					while ((ycsbMessage = ycsbReader.readLine()) != null){
						
						numPromises = 0;
						int majority = (this.getParticipants().size() + 1)/2 + 1;

						System.out.println("Read from YcSB : " + ycsbMessage);

						// Broadcast PrepareRPC to all Acceptors
						String prepareRPC = PaxosPrepareRPC();

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
								}
							}
						}

						// Testing
						System.out.println("Number of Promises: " + numPromises);
						
						// Check if Commit quorum achieved
						if (numPromises >= majority){
							// Phase 2	
						}
						else
						{
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
					leaderMessage = leaderReader.readLine();
					System.out.println("Read from LEADER : " + leaderMessage);

					//					if (leaderMessage.equals("HELLO")){
					//						System.out.println("Sending message to leader...");
					//						leaderWriter.println("Reply from " + "Server " + this.getId());
					//						leaderWriter.flush();
					//					}

					// If PaxosPrepareRPC
					if (leaderMessage.contains("PREPARE")){
						String[] extract = leaderMessage.split("\\s+");

						// Get maxRound
						String maxRoundString = extract[0].substring(extract[0].indexOf("=")+1);
						int receivedMaxRound = Integer.parseInt(maxRoundString);

						// Get Server ID
						String serverIdString = extract[1].substring(extract[1].indexOf("=")+1);
						int serverID = Integer.parseInt(serverIdString);

						// Check with LatestAcceptedProposal if received proposal is latest or not
						// If received proposal is latest
						if (receivedMaxRound > this.minProposal)
						{	
							this.minProposal = receivedMaxRound;
							// Connect to 2PC module, check for its response, send PROMISE or NACK accordingly
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

					}
				}
			}

			catch (Exception e){
				e.printStackTrace();
			}
		}
	}

	// Generate Paxos PrepareRPC
	private String PaxosPrepareRPC(){
		// Prepare message will be of the form, "PREPARE=123 ID=1"
		// Proposal no = "(maxRound + 1)" + "Server ID"
		this.maxRound++;
		String proposeMessage = "PREPARE=" + this.maxRound + " ID=" + this.id;
		return proposeMessage;
	}

	public static void main(String args[]){
		PaxosServer p1 = new PaxosServer(1);
		p1.initialize();
		p1.start();
	}
}