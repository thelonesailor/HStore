package BlockStorage;

public class Utils{

	public Utils(){

	}

	final int PAGE_SIZE = 1024 * 16;//bytes

	final int BLOCK_SIZE = 8;//pages

	final int CHUNK_SIZE = 64;//pages

	final int CACHE_SIZE = 1024 * 4;//pages

	final int BUFFER_SIZE = 1024 * 4;//pages for cache buffer & blocks for hdfs buffer

	final int SSD_SIZE = 100000;//pages

	String SSD_LOCATION = "/home/prakhar10_10/ssd"; //TODO: take as input

	public void setSSD_LOCATION(String SSD_LOCATION) {
		this.SSD_LOCATION = SSD_LOCATION;
	}

	String getSSD_LOCATION() {
		return SSD_LOCATION;
	}

	String HADOOP_USER_NAME = System.getProperty("user.name");

	String HDFS_PATH = "/HStore/"+HADOOP_USER_NAME;

}