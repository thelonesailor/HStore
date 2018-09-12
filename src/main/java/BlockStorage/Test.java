package BlockStorage;

import java.util.*;
import java.io.IOException;

public class Test{
	HDFSLayer HDFSLayer = new HDFSLayer();
	SSD SSD = new SSD(HDFSLayer);
	cache cache = new cache(SSD);
	Utils utils = new Utils();

	public void shit() {
		FileSystemOperations client = new FileSystemOperations();
		// 	block block = new block(0,new byte[8*utils.PAGE_SIZE]);
		// 	try{
//	 		client.deleteFile(0,HDFSLayer.config);
//	 	    client.addFile(HDFSLayer.config,block);
		// }
		// catch(IOException e){
		// 	e.printStackTrace();
		// }

		int numPages = 1000;
		double numBlocks = numPages*1.0 / utils.BLOCK_SIZE;

		double startTime = System.nanoTime();
		for (int i = 0; i < numBlocks; ++i)
		{
			try {
				client.deleteFile(i, HDFSLayer.config);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		double endTime = System.nanoTime();
		System.out.println((endTime-startTime)/1000000000L+" seconds");

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		startTime = System.nanoTime();
		runSeqTest(1000 ,server);
		endTime = System.nanoTime();
		System.out.println((endTime-startTime)/1000000000L+" seconds");

		startTime = System.nanoTime();
		cache.resetCache();
		endTime = System.nanoTime();
		System.out.println((endTime-startTime)/1000000000L+" seconds");

		startTime = System.nanoTime();
		SSD.resetSSD();
		endTime = System.nanoTime();
		System.out.println((endTime-startTime)/1000000000L+" seconds");

		startTime = System.nanoTime();
		HDFSLayer.flushHDFSbuffer();
		endTime = System.nanoTime();
		System.out.println((endTime-startTime)/1000000000L+" seconds");

		// 	try{
	// 	for(int i=0;i<1;i++){
	// 		client.deleteFile((long)i,HDFSLayer.config);
	// 	}
	// }
	// catch(IOException e){
	// 	e.printStackTrace();
	// }

	}
	public static void main(String args[]){
		Test t = new Test();
		t.shit();
		
	}
	/**
	* size is number of pages
	* */
	public void runSeqTest(long size,blockServer server){
		
		
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
//		hbs.commit(imageKey);
		
	}
}