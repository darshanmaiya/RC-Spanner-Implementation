package edu.ucsb.cs274.paxos;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class RedisYcsb {
	
	private Socket leaderSocket;
	
	public static void main(String args[]) throws InterruptedException{
		
		RedisYcsb r = new RedisYcsb();
		
		try{
			r.leaderSocket = new Socket("127.0.0.1", 5001);
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		// Send message to Paxos LEADER
		while(true){
			try {
				PrintWriter acceptorOutput = new PrintWriter(new OutputStreamWriter(r.leaderSocket.getOutputStream()));
				Thread.sleep(5000);
				
				// Send Commit message to test Paxos. Assuming message contains buffered writes along with Commit message 
				acceptorOutput.println("COMMIT=X:2 Y:3 Z:4");
				acceptorOutput.flush();
				acceptorOutput.println("COMMIT=X:12 Y:13 Z:14");
				acceptorOutput.flush();
				acceptorOutput.println("COMMIT=X:22 Y:23 Z:24");
				acceptorOutput.flush();
			}catch (IOException e){
				e.printStackTrace();
			}
		}
	}
}
