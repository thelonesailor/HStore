package BlockStorage;

import org.junit.Test;
import java.util.*;
import java.io.IOException;

import static org.junit.Assert.*;


public class blockServerTest {
	private Utils utils = new Utils();

	void runSeqTest(long size,blockServer server){

		byte[] b = new byte[utils.PAGE_SIZE];

		// long commitCycle = (30<<20)/BlockProtocol.clientBlockSize;
		// (size<<10)/utils.PAGE_SIZE
		for (int i=0; i<size; i++){
//		for (int i=0; i<40; i++){
			if(i%10000==0){
				System.out.println("i= " + i);
			}
//			System.out.println("addr: " + blockData.addr);
			server.writePage((long)i,b);
		}
	}

	@Test
	public void general() {

		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		FileSystemOperations client = new FileSystemOperations();
		int numPages = 1000;
		double numBlocks = numPages * 1.0 / utils.BLOCK_SIZE;

		double startTime = System.nanoTime();
		for (int i = 0; i < numBlocks; ++i) {
			try {
				client.deleteFile(i, HDFSLayer.config);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		double endTime = System.nanoTime();
		System.out.println((endTime - startTime) / 1000000000L + " seconds");

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		startTime = System.nanoTime();
		runSeqTest(1000, server);
		endTime = System.nanoTime();
		System.out.println((endTime - startTime) / 1000000000L + " seconds");

		startTime = System.nanoTime();
		cache.resetCache();
		endTime = System.nanoTime();
		System.out.println((endTime - startTime) / 1000000000L + " seconds");

		startTime = System.nanoTime();
		SSD.resetSSD();
		endTime = System.nanoTime();
		System.out.println((endTime - startTime) / 1000000000L + " seconds");

		startTime = System.nanoTime();
		HDFSLayer.flushHDFSbuffer();
		endTime = System.nanoTime();
		System.out.println((endTime - startTime) / 1000000000L + " seconds");
	}
}