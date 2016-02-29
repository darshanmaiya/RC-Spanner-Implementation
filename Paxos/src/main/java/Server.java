
public class Server {

	private int id;
	private String ipAddress;
	private int port;
	
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	public int getId(){
		return id;
	}
	
	public void setId(int id){
		this.id = id;
	}
	
	// Server constructor
	public Server(int id, String ipAddress, int port){
		this.id = id;
		this.ipAddress = ipAddress;
		this.port = port;
	}
	
	public static void main(String args[]){
		PaxosServer p2 = new PaxosServer(2);
		p2.initialize();
		p2.start();
	}
}
