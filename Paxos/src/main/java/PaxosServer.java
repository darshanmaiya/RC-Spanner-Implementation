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
//	private int 						numPaxosServers;
//	private HashMap<Integer, Socket>	clientSockets;

	// Getters and Setters
	public String getState(){ return this.state; }
	public int getMaxRound(){ return this.maxRound; }
	public int getId(){ return this.id; }
	public void setLeader(Server leader) {this.leader = leader;}
	public List<Server> getParticipants(){ return this.participants; }

	// Constructors
	public PaxosServer(){}

	// Always use this constructor to initialize a new PaxosServer instance
	public PaxosServer(int id){
		this.id = id;

		if (this.id == 1){
			this.state = "LEADER";
		}else { this.state = "ACCEPTOR";}

		this.maxRound = 0;
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
					this.participants.add(newServer);

					// If parsed Server is a LEADER
					if (Integer.parseInt(serverInfo[0]) == 1){
						this.leader = newServer;
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
				this.serverSocket = new ServerSocket(this.port);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Wait for connections from ACCEPTOR PaxosServers
			while(true){
				try {
					Socket clientSocket = null;
					clientSocket = serverSocket.accept();
					new Thread(new PaxosAcceptorHandler(clientSocket, this)).start();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		// Else: Server is an ACCEPTOR, connect to LEADER
		else{
			try{
				Socket clientConnection = new Socket(leader.getIpAddress(), leader.getPort());
				// Read Server
				BufferedReader input = new BufferedReader(new InputStreamReader(clientConnection.getInputStream()));

				String responseLine;
				
				while ((responseLine = input.readLine()) != null)
				System.out.println(String.valueOf(this.id) + " : Server : " + responseLine);
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String args[]){
		PaxosServer p1 = new PaxosServer(1);
		p1.initialize();
		p1.start();
	}
}