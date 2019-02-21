package BlockStorage;

import org.junit.Test;

public class blockServerTest {

	@Test
	public void demo1(){
		Utils utils = new Utils(8, 10, 2, true);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		cache cache = new cache(SSD, utils);
		blockServer server = new blockServer(cache, SSD, HDFSLayer, utils);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<utils.CACHE_SIZE + utils.SSD_SIZE + utils.HDFS_BUFFER_SIZE*utils.BLOCK_SIZE; ++i) {
			server.writePage(i, b);
			server.printBlockServerStatus();
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		server.stop();
		System.out.println("------------------------------------------------------");
	}



	@Test
	public void correctness1(){
		Utils utils = new Utils(256, 512, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		cache cache = new cache(SSD, utils);
		blockServer server = new blockServer(cache, SSD, HDFSLayer, utils);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<=utils.CACHE_SIZE ; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		long one = 1;
		assert !server.pageIndex.get(one).isLocationCache();
		assert server.pageIndex.get(one).isLocationSSD();
		assert !server.pageIndex.get(one).isLocationHDFS();
		assert server.pageIndex.get(one).isDirty();


		server.stop();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void correctness2(){
		Utils utils = new Utils(512, 1024, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		cache cache = new cache(SSD, utils);
		blockServer server = new blockServer(cache, SSD, HDFSLayer, utils);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<=utils.CACHE_SIZE; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}
		server.stablize();

		long one = 1;
		assert !server.pageIndex.get(one).isLocationCache();
		assert server.pageIndex.get(one).isLocationSSD();
		assert !server.pageIndex.get(one).isLocationHDFS();
		assert server.pageIndex.get(one).isDirty();

		for (int i=utils.CACHE_SIZE + 1; i<=utils.CACHE_SIZE + 1 + utils.SSD_SIZE; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(5000);}
		catch(InterruptedException e){}

		assert !server.pageIndex.get(one).isLocationCache();
		assert !server.pageIndex.get(one).isLocationSSD();
		assert server.pageIndex.get(one).isLocationHDFS();
//		assert server.pageIndex.get(one).isDirty();


		server.stop();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void correctness3(){
		Utils utils = new Utils(1024, 1648, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		cache cache = new cache(SSD, utils);
		blockServer server = new blockServer(cache, SSD, HDFSLayer, utils);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<=utils.CACHE_SIZE; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(1000);}
		catch(InterruptedException e){}

		long one = 1;
		assert !server.pageIndex.get(one).isLocationCache();
		assert server.pageIndex.get(one).isLocationSSD();
		assert !server.pageIndex.get(one).isLocationHDFS();
//		assert server.pageIndex.get(one).isDirty();

		for (int i=utils.CACHE_SIZE + 1; i<=utils.CACHE_SIZE + 1 + utils.SSD_SIZE; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(4000);}
		catch(InterruptedException e){}

		assert !server.pageIndex.get(one).isLocationCache();
		assert !server.pageIndex.get(one).isLocationSSD();
		assert server.pageIndex.get(one).isLocationHDFS();
//		assert server.pageIndex.get(one).isDirty();

		for (int i=utils.CACHE_SIZE + 2 + utils.SSD_SIZE; i<=utils.CACHE_SIZE + 2 + utils.SSD_SIZE + (utils.HDFS_BUFFER_SIZE<<3); ++i) {
			server.writePage(i, b);
		}

		try{Thread.sleep(4000);}
		catch(InterruptedException e){}

		assert !server.pageIndex.get(one).isLocationCache();
		assert !server.pageIndex.get(one).isLocationSSD();
		assert server.pageIndex.get(one).isLocationHDFS();
//		assert !server.pageIndex.get(one).isDirty();

		server.readPage(one);
		assert server.pageIndex.get(one).isLocationCache();
		assert !server.pageIndex.get(one).isLocationSSD();
		assert server.pageIndex.get(one).isLocationHDFS();
		assert !server.pageIndex.get(one).isDirty();

		server.stop();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromBlockServer1(){
		Utils utils = new Utils(256, 512, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		cache cache = new cache(SSD, utils);
		blockServer server = new blockServer(cache, SSD, HDFSLayer, utils);

		int numPages = 2000;
		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.stablize();
		server.printBlockServerStatus();

		startTime = System.nanoTime();
		for(int i=0;i<numPages;++i){
			server.readPage(i);
//			server.stablize();
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.stop();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromBlockServer2(){
		Utils utils = new Utils(1024, 2048, 128, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		cache cache = new cache(SSD, utils);
		blockServer server = new blockServer(cache, SSD, HDFSLayer, utils);

		int numPages = 3000;

		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.stablize();

		startTime = System.nanoTime();
		for(int i=0;i<numPages;++i){
			server.readPage(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.stop();
		System.out.println("------------------------------------------------------");
	}

//	@Test
//	public void WriteAndReadFromBlockServer3(){
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		cache cache = new cache(SSD);
//
//		int numPages = 100000;
//		blockServer server = new blockServer(cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//
//		double startTime = System.nanoTime();
//		byte[] b = new byte[utils.PAGE_SIZE];
//		for (int i=1; i<=numPages; ++i) {
//			server.writePage(i, b);
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=1;i<=numPages;++i){
//			server.readPage(i);
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		System.out.println("------------------------------------------------------");
//	}

	@Test
	public void WriteAndRevReadFromBlockServer2(){
		Utils utils = new Utils(256, 512, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		cache cache = new cache(SSD, utils);
		blockServer server = new blockServer(cache, SSD, HDFSLayer, utils);

		int numPages = 2000;
		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		try{Thread.sleep(4000);}
		catch(InterruptedException e){}
		server.stablize();

		startTime = System.nanoTime();
		for(int i=numPages-1;i>=0;--i){
			server.readPage(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.stop();
		System.out.println("------------------------------------------------------");
	}

//	@Test
//	public void WriteAndRevReadFromBlockServer3(){
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		cache cache = new cache(SSD);
//
//		int numPages = 100000;
//		blockServer server = new blockServer(cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//
//		double startTime = System.nanoTime();
//		byte[] b = new byte[utils.PAGE_SIZE];
//		for (int i=1; i<=numPages; ++i) {
//			server.writePage(i, b);
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=numPages;i>=1;--i){
//			server.readPage(i);
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		System.out.println("------------------------------------------------------");
//	}
}