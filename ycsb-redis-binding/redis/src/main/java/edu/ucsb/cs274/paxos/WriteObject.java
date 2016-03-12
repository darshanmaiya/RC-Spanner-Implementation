package edu.ucsb.cs274.paxos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WriteObject implements Serializable {
	
    private static final long serialVersionUID = 5804209503548575233L;
	private long transactionId;
	private List<Message> messages = new ArrayList<Message>();
	private Command command;
	private int maxVal;
	
	public List<Message> getMessages() { return this.messages; }
    public long getTransactionId() { return this.transactionId; }
    public void setMessage(List<Message> messages) { this.messages = messages; }
    public void setTransactionId(long transactionId) { this.transactionId = transactionId; }
    public void setMaxVal(int maxVal) { this.maxVal = maxVal; }
    public int getMaxVal() { return this.maxVal;}
	
    public Command getCommand(){
    	return this.command;
    }
    
	public WriteObject(Command command, long transactionId, List<Message> messages) {
		this.command = command;
		this.transactionId = transactionId;
		this.messages = messages;
	}
	
	public WriteObject(Command command){
		this.command = command;
		this.messages = null;
		
	}
	
	public WriteObject(Command command, long transactionId) {
		this.command = command;
		this.transactionId = transactionId;
	}
	
	public WriteObject(Command command, long transactionId, int maxVal) {
		this.command = command;
		this.transactionId = transactionId;
		this.maxVal = maxVal;
	}
	
	public WriteObject(Command command, long transactionId, List<Message> messages, int maxVal) {
		this.command = command;
		this.transactionId = transactionId;
		this.messages = messages;
		this.maxVal = maxVal;
	}

}
