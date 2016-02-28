import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import redis.clients.jedis.*;

public class MapStateMachine1 extends StateMachine implements Snapshottable {
	private Jedis jedis = new Jedis("localhost", 6001);

	public void put(Commit<PutCommand> commit) {
		try {
			jedis.set(String.valueOf(commit.operation().key()), String.valueOf(commit.operation().value()));
		} finally {
			commit.close();
		}
	}

	public Object get(Commit<GetQuery> commit) {
		try {
			return jedis.get(String.valueOf(commit.operation().key()));
		} finally {
			commit.close();
		}
	}

	// Snapshottable for log compaction

	  public void snapshot(SnapshotWriter writer) {
	    //writer.writeObject(map);
	  }

	  public void install(SnapshotReader reader) {
	   // map = reader.readObject();
	  } 
}
