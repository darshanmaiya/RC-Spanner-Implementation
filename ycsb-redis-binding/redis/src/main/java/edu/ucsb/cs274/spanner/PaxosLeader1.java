package edu.ucsb.cs274.spanner;

import edu.ucsb.cs274.paxos.Command;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.WriteObject;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;

public class PaxosLeader1{

	int portNum;
	private Jedis jedis;
	HashMap<String, Long> locks;


	PaxosLeader1(){
		locks = new HashMap<>();
	}

	public static void main(String[] args) {

		// Open port for connection
		try {
			ServerSocket server = new ServerSocket(8000);
			while (true) {
				Socket client = server.accept();
				(new Thread(new RequestHandler(client, 1))).start();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
}
