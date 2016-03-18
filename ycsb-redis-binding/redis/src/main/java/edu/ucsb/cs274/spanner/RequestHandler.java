package edu.ucsb.cs274.spanner;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import edu.ucsb.cs274.paxos.Command;
import edu.ucsb.cs274.paxos.Message;
import edu.ucsb.cs274.paxos.WriteObject;
import redis.clients.jedis.Jedis;

public class RequestHandler implements Runnable{
	int                       id;
	Socket                    client;
	int                       maxRound;
	Socket                    acceptorOne;
	Socket                    acceptorTwo;
	Jedis                     jedis;
	HashMap<String, Long>     locks;

	public RequestHandler(Socket client, int id){
		this.id = id;
		this.maxRound = 0;
		this.client = client;
		this.locks = new HashMap<>();
	}

	public void run(){
		try {

			// Initializing Paxos leader

			// Create input/output streams to TwoPC coordinator
			ObjectInputStream twoPcReader = new ObjectInputStream(client.getInputStream());
			ObjectOutputStream twoPcWriter = new ObjectOutputStream(client.getOutputStream());

			// Read config.properties file. Connect to Paxos Acceptors(Servers) [2 in our case]
			Properties properties = new Properties();
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			InputStream is = classloader.getResourceAsStream("config.properties");
			try {
				properties.load(is);
			} catch (IOException e) {	
				e.printStackTrace();
				return;
			}

			// First connect to redis
			jedis = new Jedis(properties.getProperty("redisServer" + String.valueOf(this.id)), Integer.valueOf(properties.getProperty("spannerRedisPort" + String.valueOf(this.id))));
			jedis.connect();


			// Connect to corresponding acceptors
			if (1 == this.id){
				this.acceptorOne = new Socket(properties.getProperty("dataCenterIp2"), 7001);
				this.acceptorTwo = new Socket(properties.getProperty("dataCenterIp3"), 7001);
			}

			if (2 == this.id){
				this.acceptorOne = new Socket(properties.getProperty("dataCenterIp1"), 7002);
				this.acceptorTwo = new Socket(properties.getProperty("dataCenterIp3"), 7002);
			}

			if (3 == this.id){
				this.acceptorOne = new Socket(properties.getProperty("dataCenterIp1"), 7003);
				this.acceptorTwo = new Socket(properties.getProperty("dataCenterIp2"), 7003);
			}

			ObjectOutputStream acceptorOneWriter = new ObjectOutputStream(acceptorOne.getOutputStream());
			ObjectOutputStream acceptorTwoWriter = new ObjectOutputStream(acceptorTwo.getOutputStream());

			ObjectInputStream acceptorOneReader = new ObjectInputStream(acceptorOne.getInputStream());
			ObjectInputStream acceptorTwoReader = new ObjectInputStream(acceptorTwo.getInputStream());

			WriteObject prepareRPC;
			WriteObject acceptRPC;
			WriteObject abortRPC;
			WriteObject acceptorOneMessage;
			WriteObject acceptorTwoMessage;
			WriteObject twoPcMessage;
			WriteObject twoPcSecondMessage;
			int majority = 2;
			int numPromises;
			int readSuccess;

			while (true){

				// Read message from 2pc coordinator
				twoPcMessage = (WriteObject)twoPcReader.readObject();

				if (twoPcMessage.getCommand() == Command.COMMIT){
					numPromises = 0;
					long txn = twoPcMessage.getTransactionId();

					// First, get the locks

					for(Message m: twoPcMessage.getMessages()) {
						String key = m.getKey();
						char keyId = m.getKey().charAt(m.getKey().length()-1);
						int shardNo = (Integer.valueOf(keyId))%3 + 1;
						if(this.id != shardNo)
							continue;
						synchronized (locks) {
							while(locks.containsKey(key)){
								if(locks.get(key) >= txn){
									break;
								}
								locks.wait();
							}
							locks.put(key, txn);
						}
					}

					// Generate PaxosPrepare RPC
					prepareRPC = PaxosPrepareRPC(twoPcMessage);

					// Send prepare RPC to acceptor 1
					acceptorOneWriter.writeObject(prepareRPC);
					acceptorOneWriter.flush();

					acceptorOneMessage = (WriteObject)acceptorOneReader.readObject();
					if (acceptorOneMessage.getCommand() == Command.PROMISE)	
						numPromises++;

					// Send prepare RPC to acceptor 2
					acceptorTwoWriter.writeObject(prepareRPC);
					acceptorTwoWriter.flush();

					acceptorTwoMessage = (WriteObject)acceptorTwoReader.readObject();
					if (acceptorOneMessage.getCommand() == Command.PROMISE)	
						numPromises++;

					if ((numPromises + 1) >= majority){
						numPromises = 0;
						// Send SUCCESS to 2PC
						twoPcWriter.writeObject(new WriteObject(Command.SUCCESS));
						twoPcWriter.flush();

						// Read reply from 2PC
						twoPcSecondMessage = (WriteObject)twoPcReader.readObject();

						if (twoPcSecondMessage.getCommand() == Command.ACCEPT){
							// Continue processing Phase 2 here
							acceptRPC = PaxosAcceptRPC(twoPcMessage);

							// First log and write in its own shard, Send to all acceptors
							for (Message m : twoPcMessage.getMessages()){

								String key = m.getKey();

								char keyId = key.charAt(key.length()-1);
								int shardNo = (Integer.valueOf(keyId))%3 + 1;

								// If its own shard, then write
								if (shardNo == this.id){
									 synchronized (locks) {
						                    locks.put(m.getKey(), txn);

									jedis.hmset(m.getKey(), m.getValues());
									 }
								}
							}

							// Send Accept to all acceptors
							acceptorOneWriter.writeObject(acceptRPC);
							acceptorOneWriter.flush();

							acceptorTwoWriter.writeObject(acceptRPC);
							acceptorTwoWriter.flush();

							// Release the locks
							for(Message m: twoPcMessage.getMessages()){
				                  char keyId = m.getKey().charAt(m.getKey().length()-1);
				                  int shardNo = ((Integer.valueOf(keyId))%3) + 1;
				                  
				                  if(this.id != shardNo)
				                    continue;
				                  
				                  synchronized (locks){
				                    locks.remove(m.getKey());
				                    locks.notifyAll();
				                  }
				                }
						
							twoPcWriter.writeObject(new WriteObject(Command.SUCCESS));
							twoPcWriter.flush();

						}
					}
					else{
						numPromises = 0;
						twoPcWriter.writeObject(new WriteObject(Command.FAILURE));
						twoPcWriter.flush();
					}
				}

				// If READ
				if (twoPcMessage.getCommand() == Command.READ){

					readSuccess = 0;

					// Send read message to acceptor 1
					acceptorOneWriter.writeObject(twoPcMessage);
					acceptorOneWriter.flush();

					// Send read message to acceptor 2
					acceptorTwoWriter.writeObject(twoPcMessage);
					acceptorTwoWriter.flush();

					// Check for consistent read from replicated shards
					acceptorOneMessage = (WriteObject)acceptorOneReader.readObject();
					acceptorTwoMessage = (WriteObject)acceptorTwoReader.readObject();

					if (acceptorOneMessage.getCommand() == Command.SUCCESS){
						readSuccess++;
					}

					if (acceptorTwoMessage.getCommand() == Command.SUCCESS){
						readSuccess++;
					}

					if ((readSuccess + 1) >= majority){
						if (acceptorOneMessage.getCommand() == Command.SUCCESS){
							twoPcWriter.writeObject(acceptorOneMessage);
							twoPcWriter.flush();
						}
						else{
							twoPcWriter.writeObject(acceptorTwoMessage);
							twoPcWriter.flush();
						}
					}
					else{
						twoPcWriter.writeObject(new WriteObject(Command.FAILURE));
						twoPcWriter.flush();
					}
				}
			}
		} 
		catch (EOFException e){
			//e.printStackTrace();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	// Generate Paxos PrepareRPC
	private WriteObject PaxosPrepareRPC(WriteObject ycsbMessage) {
		// Prepare message will be of the form, "PREPARE=123 COMMIT=X:2 Y:3 Z:4"
		// Proposal no = "(maxRound + 1)"
		this.maxRound++;

		WriteObject proposeObject = new WriteObject(Command.PREPARE, ycsbMessage.getTransactionId(), ycsbMessage.getMessages(), this.maxRound);

		return proposeObject;
	}

	// Generate Paxos AcceptRPC
	private WriteObject PaxosAcceptRPC(WriteObject ycsbMessage) {
		// Accept message will be of the form, "ACCEPT=1 COMMIT=X:2 Y:3 Z:4"

		WriteObject acceptRPC = new WriteObject(Command.ACCEPT, ycsbMessage.getTransactionId(), ycsbMessage.getMessages(), this.maxRound);

		return acceptRPC;
	}

	// Generate Paxos AbortRPC
	private WriteObject PaxosAbortRPC(long transactionId) {
		// Abort message will be of the form, "ABORT=1 COMMIT=X:2 Y:3 Z:4"

		WriteObject abortRPC = new WriteObject(Command.ABORT, transactionId);

		return abortRPC;
	}
}