
public class PaxosPrepare {

	private String 	type;				// Type: REQUEST (or) RESPONSE
	private int 	proposalNumber;
	private String 	key;
	private String 	value;
	
	public PaxosPrepare(int maxRound, int id){
		// Proposal no = (maxRound + 1)+ Server ID
		this.proposalNumber = maxRound + id;
		this.type = "REQUEST";
	}
	
	public static void main(String args[]){
		PaxosServer p3 = new PaxosServer(3);
		p3.initialize();
		p3.start();
	}
}
