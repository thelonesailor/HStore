package BlockStorage;

public class Utils{

	public Utils(){

	}

	final int PAGE_SIZE = 1024 * 16;

	final int BLOCK_SIZE = 8;

	final int CHUNK_SIZE = 64;

	final int CACHE_SIZE = 1024 * 4;

	final int BUFFER_SIZE = 1024 * 4;

	final int SSD_SIZE = 100000;

	String SSD_LOCATION = "/home/prakhar10_10/sds";

	String USER_NAME = "prakhar10_10";

	public void setSSD_LOCATION(String SSD_LOCATION) {
		this.SSD_LOCATION = SSD_LOCATION;
	}

	String getSSD_LOCATION() {
		return SSD_LOCATION;
	}

}