import java.io.*;
import java.net.Socket;

public class Server {

	private int id;
	private String ipAddress;
	private int port;
	private Socket acceptorSocket;
	private PrintWriter acceptorWriter;
	private BufferedReader acceptorReader;
	private boolean acceptedPrepare;

	// Server constructor
	public Server(int id, String ipAddress, int port){
		this.id = id;
		this.ipAddress = ipAddress;
		this.port = port;
		this.acceptedPrepare = false;
	}
	
	public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
	public int getPort() { return port; }
	public void setPort(int port) { this.port = port; }
	public int getId(){ return id; }
	public void setId(int id){ this.id = id; }
	public String getIpAddress() { return ipAddress; }
	public Socket getAcceptorSocket() { return acceptorSocket; }
	public void setAcceptorSocket(Socket acceptorSocket) { this.acceptorSocket = acceptorSocket; }
	public PrintWriter getAcceptorWriter() { return acceptorWriter; }
	public void setAcceptorWriter(PrintWriter acceptorWriter) { this.acceptorWriter = acceptorWriter; }
	public BufferedReader getAcceptorReader() { return acceptorReader; }
	public void setAcceptorReader(BufferedReader acceptorReader) { this.acceptorReader = acceptorReader; }
	public boolean getAcceptedPrepare() { return acceptedPrepare; }
	public void setAcceptedPrepare(boolean acceptedPrepare) { this.acceptedPrepare = acceptedPrepare; }

	public static void main(String args[]){
		PaxosServer p2 = new PaxosServer(2);
		p2.initialize();
		p2.start();
	}
}
