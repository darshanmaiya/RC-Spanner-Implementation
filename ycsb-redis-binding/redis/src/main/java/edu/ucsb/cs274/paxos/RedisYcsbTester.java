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
	private Socket leaderSocket2;
	private ObjectInputStream inputStream2 = null;
	private ObjectOutputStream outputStream2 = null;
	
	
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
			
			Message newMessage = new Message(Command.COMMIT, "user1000", null, values);
			messageList.add(newMessage);
						
			redisYcsb.outputStream = new ObjectOutputStream(redisYcsb.leaderSocket.getOutputStream());
			redisYcsb.inputStream = new ObjectInputStream(redisYcsb.leaderSocket.getInputStream());
			
			redisYcsb.outputStream.writeObject(
					new WriteObject(Command.COMMIT,
									  1, // Have to change this Transaction id, using 1 for Testing
									  messageList));	
			redisYcsb.outputStream.flush();
		
            received = (WriteObject) redisYcsb.inputStream.readObject();
            System.out.println("Object received with details for write:\n" + received);
            	
            messageList.add(0, new Message(Command.READ, "user1000"));
			redisYcsb.outputStream.writeObject(
					new WriteObject(Command.READ,
									  1, // Have to change this Transaction id, using 1 for Testing
									  messageList));	
			redisYcsb.outputStream.flush();
			
			received = (WriteObject) redisYcsb.inputStream.readObject();
            System.out.println("Object received with details for read:\n" + received.getMessages());
               
            // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            			
            try{
    			redisYcsb.leaderSocket2 = new Socket("127.0.0.1", 5000);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
            
            redisYcsb.outputStream2 = new ObjectOutputStream(redisYcsb.leaderSocket2.getOutputStream());
			redisYcsb.inputStream2 = new ObjectInputStream(redisYcsb.leaderSocket2.getInputStream());
            
			HashMap<String, String> values2 = new HashMap<>();
			values2.put("f1", new String("value for f1"));
			values2.put("f2", new String("value for f2"));
			values2.put("f3", new String("value for f3"));
			values2.put("f4", new String("value for f4"));
			values2.put("f5", new String("value for f5"));
			values2.put("f6", new String("value for f6"));
			
			
			List<Message> messageList2 = new ArrayList<Message>();
			Message newMessage2 = new Message(Command.COMMIT, "user2000", null, values2);
			messageList2.add(newMessage2);
		//	System.out.println(newMessage2);
			
			redisYcsb.outputStream2.writeObject(
					new WriteObject(Command.COMMIT,
									  2, // Have to change this Transaction id, using 1 for Testing
									  messageList2));	
			redisYcsb.outputStream2.flush();
			
            received = (WriteObject) redisYcsb.inputStream2.readObject();
            System.out.println("Object received with details for write:\n" + received);
            
			messageList2.add(0, new Message(Command.READ, "user2000"));
			redisYcsb.outputStream2.writeObject(
					new WriteObject(Command.READ,
									  1, // Have to change this Transaction id, using 1 for Testing
									  messageList2));	
			redisYcsb.outputStream2.flush();
			
			received = (WriteObject) redisYcsb.inputStream2.readObject();
            System.out.println("Object received with details for read:\n" + received.getMessages());

            while (true)
                ;
            
            // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
