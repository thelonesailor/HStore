package blockstorage;

import org.jetbrains.annotations.NotNull;

public class Utils{

	boolean SHOW_LOG;

	final int PAGE_SIZE = 1024 * 16;//bytes   16KB

	final int BLOCK_SIZE = 8;//pages   128KB

	int CACHE_SIZE; //= 1024 * 4;//pages   64MB
	final int MAX_CACHE_FULL_PERCENTAGE = 70;// %
	int MAX_CACHE_FULL_SIZE;

//	int HDFS_BUFFER_SIZE = 1024 * 4;//blocks for hdfs buffer(512MB)
	int HDFS_BUFFER_SIZE ;//blocks for hdfs buffer(128MB)

//	int SSD_SIZE = 100000;//pages   ~1.6GB
	int SSD_SIZE = 10000;//pages   ~.16GB
	final int MAX_SSD_FULL_PERCENTAGE = 70;// %
	int MAX_SSD_FULL_SIZE;


	String SSD_LOCATION = System.getenv("HOME") + "/ssd";

	public void setSSD_LOCATION(String SSD_LOCATION) {
		this.SSD_LOCATION = SSD_LOCATION;
	}

	String getSSD_LOCATION() {
		return SSD_LOCATION;
	}

	String HADOOP_USER_NAME = System.getProperty("user.name");

	@NotNull String HDFS_PATH = "/HStore/"+HADOOP_USER_NAME;


	public Utils(int cache_size, int ssd_size, int hdfs_buffer_size, boolean show_log){
		CACHE_SIZE = cache_size;
		SSD_SIZE = ssd_size;
		HDFS_BUFFER_SIZE = hdfs_buffer_size;
		SHOW_LOG = show_log;
		MAX_CACHE_FULL_SIZE = (MAX_CACHE_FULL_PERCENTAGE * CACHE_SIZE)/100;
		MAX_SSD_FULL_SIZE = (MAX_SSD_FULL_PERCENTAGE * SSD_SIZE)/100;
	}
}