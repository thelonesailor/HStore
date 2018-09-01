package BlockStorage;

public class Utils{

    public Utils(){

    }

    public final int CACHE_SIZE = 1024 * 4;

    public final int PAGE_SIZE = 1024 * 16;

    public final int CHUNK_SIZE = 64;

    public final int BLOCK_SIZE = 8;

    public String SSD_LOCATION = "/home/vivek/Desktop/sds";
    
    public final int SSD_SIZE = 100000;

    public void setSSD_LOCATION(String SSD_LOCATION) {
        this.SSD_LOCATION = SSD_LOCATION;
    }
    
    public String getSSD_LOCATION() {
		return SSD_LOCATION;
	}
    
}