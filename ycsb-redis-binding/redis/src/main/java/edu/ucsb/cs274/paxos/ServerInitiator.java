package edu.ucsb.cs274.paxos;

import java.net.ServerSocket;
import java.net.Socket;

public class ServerInitiator {

	public static void main(String[] args){

		try {
			ServerSocket listener = new ServerSocket(5001);

			while(true) {
				Socket request = listener.accept();
        request.setPerformancePreferences(0,2,1);
        request.setTcpNoDelay(true);
				(new Thread(new Server(request, 1))).start();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
}

