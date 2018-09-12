package BlockStorage;

public class Utils{

	public Utils(){

	}

	final int PAGE_SIZE = 1024 * 16;

	final int BLOCK_SIZE = 8;//pages

	final int CHUNK_SIZE = 64;

	final int CACHE_SIZE = 1024 * 4;

	final int BUFFER_SIZE = 1024 * 4;

	final int SSD_SIZE = 100000;

	String SSD_LOCATION = "/home/prakhar10_10/ssd";

	public void setSSD_LOCATION(String SSD_LOCATION) {
		this.SSD_LOCATION = SSD_LOCATION;
	}

	String getSSD_LOCATION() {
		return SSD_LOCATION;
	}

	String HADOOP_USER_NAME = "prakhar10_10";

	String HDFS_LOCATION = "hdfs://localhost:9000/HStore/"+HADOOP_USER_NAME;

	public void setHDFS_LOCATION(String HDFS_LOCATION) {
		this.HDFS_LOCATION = HDFS_LOCATION;
	}

	String getHDFS_LOCATION() { return HDFS_LOCATION; }

}