import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class RedisYcsb {
	
	private ServerSocket serverSocket;
	
	public static void main(String args[]) throws InterruptedException{
		
		RedisYcsb r = new RedisYcsb();
	//	r.serverSocket = new ServerSocket(5000);
		
		try{
			r.serverSocket = new ServerSocket(6000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	//	 Send message to Paxos LEADER
		while(true){
			try {
				Socket leaderSocket = null;
				leaderSocket = r.serverSocket.accept();
				PrintWriter acceptorOutput = new PrintWriter(new OutputStreamWriter(leaderSocket.getOutputStream()));
				Thread.sleep(10000);
			//	acceptorOutput.println("HELLO");
				// Send Commit message to test Paxos. Assuming message contains buffered writes along with Commit message 
				acceptorOutput.println("COMMIT=X:2 Y:3 Z:4");
				acceptorOutput.flush();
				acceptorOutput.println("COMMIT=X:12 Y:13 Z:14");
				acceptorOutput.flush();
				acceptorOutput.println("COMMIT=X:12 Y:13 Z:14");
				acceptorOutput.flush();
			}catch (IOException e){
				e.printStackTrace();
			}
		}
	}
}