package edu.ucsb.cs274.paxos;

import java.net.ServerSocket;
import java.net.Socket;

public class Server2Initiator {
	
	public static void main(String[] args){

		try {
			ServerSocket listener = new ServerSocket(5002);

			while(true) {
				Socket request = listener.accept();
				(new Thread(new Server(request, 2))).start();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

}
