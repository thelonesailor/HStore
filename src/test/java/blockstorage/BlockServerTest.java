package blockstorage;

import org.junit.Test;

import java.io.IOException;

public class BlockServerTest {

	@Test
	public void RecoveryTest(){
		Utils utils = new Utils(2000, 4000, 256, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		try{
			server.recover();
			server.printBlockServerStatus();
		}
		catch(IOException e){
			System.out.println("IOException in recover() "+e);
		}

		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void demo1(){
		Utils utils = new Utils(16, 24, 2, true);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<utils.CACHE_SIZE + utils.SSD_SIZE + utils.HDFS_BUFFER_SIZE*utils.BLOCK_SIZE; ++i) {
			server.writePage(i, b);
//			server.printBlockServerStatus();
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void correctness1(){
		Utils utils = new Utils(256, 512, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<=utils.CACHE_SIZE ; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		int one = 1;
		assert !server.pageIndex.get(one).isLocationCache();
		assert server.pageIndex.get(one).isLocationSSD();
		assert !server.pageIndex.get(one).isLocationHDFS();
		assert server.pageIndex.get(one).isDirty();


		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}
	
	@Test
	public void correctness2(){
		Utils utils = new Utils(512, 1024, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<=utils.CACHE_SIZE; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}
		server.stabilize();

		int one = 1;
		assert !server.pageIndex.get(one).isLocationCache();
		assert server.pageIndex.get(one).isLocationSSD();
		assert !server.pageIndex.get(one).isLocationHDFS();
		assert server.pageIndex.get(one).isDirty();

		for (int i=utils.CACHE_SIZE + 1; i<=utils.CACHE_SIZE + 1 + utils.SSD_SIZE; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(4000);}
		catch(InterruptedException e){}
		server.stabilize();

		assert !server.pageIndex.get(one).isLocationCache();
		assert !server.pageIndex.get(one).isLocationSSD();
		assert server.pageIndex.get(one).isLocationHDFS();
//		assert server.pageIndex.get(one).isDirty();


		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void correctness3(){
		Utils utils = new Utils(1024, 1648, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<=utils.CACHE_SIZE; ++i) {
			server.writePage(i, b);
		}
		try{Thread.sleep(1000);}
		catch(InterruptedException e){}

		int one = 1;
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

		try{Thread.sleep(3000);}
		catch(InterruptedException e){}
		server.stabilize();

		assert !server.pageIndex.get(one).isLocationCache();
		assert !server.pageIndex.get(one).isLocationSSD();
		assert server.pageIndex.get(one).isLocationHDFS();
//		assert server.pageIndex.get(one).isDirty();

		server.readPage(one);
		try{Thread.sleep(1000);}
		catch(InterruptedException e){}
		server.stabilize();
		assert server.pageIndex.get(one).isLocationCache();
		assert !server.pageIndex.get(one).isLocationSSD();
		assert server.pageIndex.get(one).isLocationHDFS();
		assert !server.pageIndex.get(one).isDirty();

		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromBlockServer1(){
		Utils utils = new Utils(200, 400, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		int numPages = 1300;
		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.stabilize();
		server.printBlockServerStatus();

		startTime = System.nanoTime();
		for(int i=0;i<numPages;++i){
			server.readPage(i);
		}
		server.stabilize();
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromBlockServer2(){
		Utils utils = new Utils(924, 1848, 128, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		int numPages = 2700;

		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.stabilize();

		startTime = System.nanoTime();
		for(int i=0;i<numPages;++i){
			server.readPage(i);
		}
		server.stabilize();
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndRevReadFromBlockServer2(){
		Utils utils = new Utils(256, 512, 64, false);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);
		server.vMmanager.registerVM(4000);

		int numPages = 1400;
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
		server.stabilize();

		startTime = System.nanoTime();
		for(int i=numPages-1;i>=0;--i){
			server.readPage(i);
		}
		server.stabilize();
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}
}