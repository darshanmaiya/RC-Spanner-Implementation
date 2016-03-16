package edu.ucsb.cs274.paxos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Message implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5804209503548575232L;
	private Command command;
	private String key = null;
	// This is only for internal message value passing and not for YCSB-Redis
	private String value = null;
	// Used while doing reads/writes
	private Set<String> fields = null;
	// Used while doing writes
	private Map<String, String> values = null;
	
	public Message (Command command) {
		this.command = command;
	}
	
	public Message (Command command, String key) {
		this.command = command;
		this.key = key;
	}
	
	public Message (Command command, String key, String value) {
		this.command = command;
		this.key = key;
		this.value = value;
	}
	
	public Message (Command command, String key, String value, Set<String> fields) {
		this.command = command;
		this.key = key;
		this.value = value;
		this.fields = fields;
	}
	
	public Message (Command command, String key, String value, Map<String, String> values) {
		this.command = command;
		this.key = key;
		this.value = value;
		this.values = values;
	}
	
	public Message (Message m) {
		this.command = m.getCommand();
		this.key = m.getKey();
		this.value = m.getValue();
		this.fields = m.getFields();
		this.values = m.getValues();
	}
	
	public Command getCommand() {
		return command;
	}
	
	public void setCommand(Command command) {
		this.command = command;
	}
	
	public Map<String, String> getValues() {
		return values;
	}
	
	public void setValues(HashMap<String, String> values) {
		this.values = values;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Set<String> getFields() {
		return fields;
	}

	public void setFields(Set<String> fields) {
		this.fields = fields;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		StringBuilder returnVal = new StringBuilder("Command: " + command.name());
		
		if(key != null)
			returnVal.append(" | key: " + key);
		
		if(value != null)
			returnVal.append(" | value: " + value);
		
		if(fields != null && !fields.isEmpty() && (values == null || values.isEmpty())) {
			returnVal.append(" | fields: ");
			
			for(String field : fields)
				returnVal.append(field + ", ");
			
			returnVal.replace(returnVal.length() - 2, returnVal.length(), "");
		}
		
		if(values != null && !values.isEmpty()) {
			returnVal.append(" | field-values: ");
			
			for (Map.Entry<String, String> entry : values.entrySet()) {
				returnVal.append(entry.getKey() + "=" + entry.getValue().toString() + ", ");
		    }
			
			returnVal.replace(returnVal.length() - 2, returnVal.length(), "");
		}
		
		return returnVal.toString();
	}
}
