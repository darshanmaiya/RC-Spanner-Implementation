import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member.Type;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

public class Server1 {

	public static void main(String args[]){

		Address address = new Address("127.0.0.1", 5001);

		Collection<Address> members = Arrays.asList(
				new Address("127.0.0.1", 5002),
				new Address("127.0.0.1", 5003)
				);

		CopycatServer server = CopycatServer.builder(address, members)
				  .withStateMachine(MapStateMachine1::new)
				  .withTransport(NettyTransport.builder()
				    .withThreads(4)
				    .build())
				  .withStorage(Storage.builder()
				    .withDirectory(new File("logs"))
				    .withStorageLevel(StorageLevel.DISK)
				    .build())
				  .withType(Type.ACTIVE)
				  .build();
		
		// Whitelisting
		server.serializer().register(PutCommand.class);
		server.serializer().register(GetQuery.class);
		
		// Start server
		CompletableFuture<CopycatServer> future = server.open();
		future.join();
	}
}
