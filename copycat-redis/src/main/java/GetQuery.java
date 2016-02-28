import io.atomix.copycat.Query;

public class GetQuery implements Query<Object> {
  /**
	 * 
	 */
	private static final long serialVersionUID = 5310985064433756428L;
	private final Object key;

	public GetQuery(Object key) {
		this.key = key;
  	}

	public Object key() {
    		return key;
	}
}
