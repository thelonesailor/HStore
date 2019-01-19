package BlockStorage;

public class Utils{

	public Utils(){

	}

	final int PAGE_SIZE = 1024 * 16;//bytes   16KB

	final int BLOCK_SIZE = 8;//pages   128KB

	final int CACHE_SIZE = 1024 * 4;//pages   64MB
	final int MAX_CACHE_FULL_PERCENTAGE = 80;// %

	final int HDFS_BUFFER_SIZE = 1024 * 4;//blocks for hdfs buffer(512MB)
//	final int HDFS_BUFFER_SIZE = 1024 * 1;//blocks for hdfs buffer(128MB)

//	final int SSD_SIZE = 100000;//pages   ~1.6GB
	final int SSD_SIZE = 10000;//pages   ~.16GB
	final int MAX_SSD_FULL_PERCENTAGE = 80;// %

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