import java.util.*;
import java.util.concurrent.CompletableFuture;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;

public class Client {

	public static void main(String args[])
	{
		Collection<Address> members = Arrays.asList(
				new Address("127.0.0.1", 5002),
				new Address("127.0.0.1", 5003),
				new Address("127.0.0.1", 5001)
				);

		CopycatClient client = CopycatClient.builder(members)
				.withTransport(NettyTransport.builder()
						.withThreads(2)
						.build())
				.build();
		
		client.serializer().register(PutCommand.class);
		client.serializer().register(GetQuery.class);
		
		CompletableFuture<CopycatClient> future = client.open();
		future.join();
		
		// Testing
		
		// Submit three PutCommands to the replicated state machine
		CompletableFuture[] futures = new CompletableFuture[3];
		futures[0] = client.submit(new PutCommand("key1", "value1"));
		futures[1] = client.submit(new PutCommand("key2", "value2"));
		futures[2] = client.submit(new PutCommand("key3", "value3"));

		// Print a message once all three commands have completed
		CompletableFuture.allOf(futures).thenRun(() -> System.out.println("Commands completed!"));
	
		client.submit(new GetQuery("key1")).thenAccept(result -> {
			  System.out.println("key1 is: " + result);
			});
		
		client.submit(new GetQuery("key2")).thenAccept(result -> {
			  System.out.println("key2 is: " + result);
			});
		
		client.submit(new GetQuery("key3")).thenAccept(result -> {
			  System.out.println("value3 is: " + result);
			});

	}
}
