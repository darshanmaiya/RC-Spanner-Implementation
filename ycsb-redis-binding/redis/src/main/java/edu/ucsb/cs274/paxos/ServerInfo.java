package edu.ucsb.cs274.paxos;
import java.io.*;
import java.net.Socket;

public class ServerInfo {

	private int id;
	private String ipAddress;
	private int port;
	private Socket acceptorSocket;
	private ObjectOutputStream acceptorWriter;
	private ObjectInputStream acceptorReader;
	private boolean acceptedPrepare;

	// Server constructor
	public ServerInfo(int id, String ipAddress, int port){
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
	public ObjectOutputStream getAcceptorWriter() { return acceptorWriter; }
	public void setAcceptorWriter(ObjectOutputStream acceptorWriter) { this.acceptorWriter = acceptorWriter; }
	public ObjectInputStream getAcceptorReader() { return acceptorReader; }
	public void setAcceptorReader(ObjectInputStream acceptorReader) { this.acceptorReader = acceptorReader; }
	public boolean getAcceptedPrepare() { return acceptedPrepare; }
	public void setAcceptedPrepare(boolean acceptedPrepare) { this.acceptedPrepare = acceptedPrepare; }
}
