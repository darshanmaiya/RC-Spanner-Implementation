package edu.ucsb.cs274.spanner;

import java.net.ServerSocket;
import java.net.Socket;

public class Server1Initiator {

	public static void main(String[] args){

		try {
			ServerSocket listener = new ServerSocket(5001);

			while(true) {
				Socket request = listener.accept();
				(new Thread(new Server(request, 1))).start();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
}

