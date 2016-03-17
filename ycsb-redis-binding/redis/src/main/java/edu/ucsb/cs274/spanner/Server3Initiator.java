package edu.ucsb.cs274.spanner;

import java.net.ServerSocket;
import java.net.Socket;

public class Server3Initiator {

	public static void main(String[] args){

		try {
			ServerSocket listener = new ServerSocket(5003);

			while(true) {
				Socket request = listener.accept();
				(new Thread(new Server(request, 3))).start();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
}
