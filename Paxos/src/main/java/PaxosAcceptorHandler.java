
import java.io.*;
import java.net.*;

public class PaxosAcceptorHandler implements Runnable{

	private Socket clientSocket = null;
	private PaxosServer paxosInstance;
	
	public PaxosAcceptorHandler(Socket clientSocket, PaxosServer paxosInstance) {
		this.clientSocket = clientSocket;
		this.paxosInstance = paxosInstance;
	}

	public void run() {
		try {
			DataInputStream input  = new DataInputStream(clientSocket.getInputStream());
			PrintWriter output = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
			
			System.out.println("This server State: " + paxosInstance.getState());
			output.println("HELLO");
			output.flush();
			System.out.println("Message sent");
		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

