package edu.ucsb.cs274.paxos;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RedisYcsbTester {
	
	private Socket leaderSocket;
	private ObjectInputStream inputStream = null;
	private ObjectOutputStream outputStream = null;
	
	
	public static void main(String args[]) throws InterruptedException{
		
		RedisYcsbTester redisYcsb = new RedisYcsbTester();
		
		try{
			redisYcsb.leaderSocket = new Socket("127.0.0.1", 5000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		// Send message to Paxos Client
		try {
			WriteObject received;
			
			HashMap<String, String> values = new HashMap<>();
			values.put("field1", new String("value for field 1"));
			values.put("field2", new String("value for field 2"));
			values.put("field3", new String("value for field 3"));
			values.put("field4", new String("value for field 4"));
			values.put("field5", new String("value for field 5"));
			values.put("field6", new String("value for field 6"));
			
			List<Message> messageList = new ArrayList<Message>();
			
			Message newMessage = new Message(Command.COMMIT, "user45678", null, values);
			messageList.add(newMessage);
						
			redisYcsb.outputStream = new ObjectOutputStream(redisYcsb.leaderSocket.getOutputStream());
			redisYcsb.outputStream.writeObject(
					new WriteObject(Command.COMMIT,
									  1, // Have to change this Transaction id, using 1 for Testing
									  messageList));	
			redisYcsb.outputStream.flush();
			
			redisYcsb.inputStream = new ObjectInputStream(redisYcsb.leaderSocket.getInputStream());
		
            received = (WriteObject) redisYcsb.inputStream.readObject();
            System.out.println("Object received with details for write:\n");
            while (true);
//			redisYcsb.outputStream.writeObject(
//					new Message(Command.READ, "user45678"));	
//			redisYcsb.outputStream.flush();
//			
//            message = (Message) redisYcsb.inputStream.readObject();
//            System.out.println("Object received with details for read:\n" + message);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
