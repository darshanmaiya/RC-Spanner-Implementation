package edu.ucsb.cs274.paxos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WriteObject implements Serializable {
	
    private static final long serialVersionUID = 5804209503548575233L;
	private long transactionId;
	private List<Message> messages;
	
	public List<Message> getMessages() { return this.messages; }
    public long getTransactionId() { return this.transactionId; }
    public void setMessage(List<Message> messages) { this.messages = messages; }
    public void setTransactionId(long transactionId) { this.transactionId = transactionId; }
	
	public WriteObject(long transactionId, List<Message> messages) {
		this.transactionId = transactionId;
		this.messages = messages;
	}
}
