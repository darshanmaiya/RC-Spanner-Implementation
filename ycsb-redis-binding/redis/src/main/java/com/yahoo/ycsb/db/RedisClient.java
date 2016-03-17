/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import edu.ucsb.cs274.paxos.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

	private Jedis jedis;

	public static final String HOST_PROPERTY = "redis.host";
	public static final String PORT_PROPERTY = "redis.port";
	public static final String PASSWORD_PROPERTY = "redis.password";

	public static final String INDEX_KEY = "_indices";

	private static int transactions = 1;

	private int transactionId;
	
	private Socket leaderSocket;
	private ObjectInputStream inputStream = null;
	private ObjectOutputStream outputStream = null;

	public void init() throws DBException {
		transactionId = transactions++;
		//System.out.println("in init redis with transaciton id: " + transactionId);
		Properties props = getProperties();
		int port;

		String portString = props.getProperty(PORT_PROPERTY);
		if (portString != null) {
			port = Integer.parseInt(portString);
		} else {
			port = Protocol.DEFAULT_PORT;
		}
		String host = props.getProperty(HOST_PROPERTY);

		jedis = new Jedis(host, port);
		jedis.connect();

		String password = props.getProperty(PASSWORD_PROPERTY);
		if (password != null) {
			jedis.auth(password);
		}
		
		try{
			leaderSocket = new Socket("127.0.0.1", 5000);
			outputStream = new ObjectOutputStream(leaderSocket.getOutputStream());
			inputStream = new ObjectInputStream(leaderSocket.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void cleanup() throws DBException {
		//System.out.println("in cleanup");
		jedis.disconnect();
	}

	/**
	 * Start a database transaction. 
	 */
	public void start() throws DBException {
		//System.out.println("start transaction with id: " + transactionId);
	}

	/**
	 * Commit the current database transaction. 
	 */
	public void commit() throws DBException {
		//System.out.println("commit transaction with id: " + transactionId);
	}

	/**
	 * Abort the current database transaction. 
	 */
	public void abort() throws DBException {
		//System.out.println("abort transaction with id: " + transactionId);
	}

	/*
	 * Calculate a hash for a key to store it in an index. The actual return value
	 * of this function is not interesting -- it primarily needs to be fast and
	 * scattered along the whole space of doubles. In a real world scenario one
	 * would probably use the ASCII values of the keys.
	 */
	public double hash(String key) {
		return key.hashCode();
	}

	// XXX jedis.select(int index) to switch to `table`

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		WriteObject writeObject;
		List<Message> messageList = new ArrayList<Message>();
		
		Message newMessage = new Message(Command.READ, key);
		messageList.add(newMessage);
		try {
			outputStream.writeObject(
					new WriteObject(Command.READ,
									  transactionId,
									  messageList));
			outputStream.flush();
			writeObject = (WriteObject) inputStream.readObject();
			StringByteIterator.putAllAsByteIterators(result, writeObject.getMessages().get(0).getValues());
		} catch (Exception e) {
			e.printStackTrace();
			// TODO: For testing
			return Status.OK;
		}
		
		return result.isEmpty() ? Status.ERROR : Status.OK;
	}

	@Override
	public Status insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		//System.out.println("in insert with key: " + key);
		WriteObject writeObject;
		List<Message> messageList = new ArrayList<Message>();
		
		Message newMessage = new Message(Command.COMMIT, key, null, StringByteIterator.getStringMap(values));
		messageList.add(newMessage);
		try {
			outputStream.writeObject(
					new WriteObject(Command.COMMIT,
									  transactionId,
									  messageList));
			outputStream.flush();
			writeObject = (WriteObject) inputStream.readObject();
			if(writeObject.getCommand() == Command.SUCCESS)
				return Status.OK;
			else
				return Status.ERROR;
		} catch (Exception e) {
			e.printStackTrace();
			// TODO: For testing
			return Status.OK;
		}
	}

	@Override
	public Status delete(String table, String key) {
		return Status.OK;
	}

	@Override
	public Status update(String table, String key,
			HashMap<String, ByteIterator> values) {
		//System.out.println("in update with key: " + key);
		return insert(table, key, values);
	}

	@Override
	public Status scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return Status.OK;
	}

}
