package BlockStorage;

import org.junit.Test;

public class SSDTest {
	private Utils utils = new Utils();
	
	@Test
	public void WriteAndReadFromSSDLayer1() {

		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 1000;

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		byte[] b = new byte[utils.PAGE_SIZE];
		double startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			SSD.writePage(new page(i, b), server);
			server.updatePageIndex(i, 0, 1, 0, 1);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages to SSDLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			SSD.readPage(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages from SSDLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromSSDLayer2() {

		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 10000;

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		byte[] b = new byte[utils.PAGE_SIZE];
		double startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			SSD.writePage(new page(i, b), server);
			server.updatePageIndex(i, 0, 1, 0, 1);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages to SSDLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			SSD.readPage(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages from SSDLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

//	@Test
//	public void WriteAndReadFromSSDLayer3() {
//
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		cache cache = new cache(SSD);
//
//		int numPages = 100000;
//
//		blockServer server = new blockServer(cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//		byte[] b = new byte[utils.PAGE_SIZE];
//		double startTime = System.nanoTime();
//		for(int i=1;i<=numPages;++i){
//			SSD.writePage(new page(i, b), server);
//			server.updatePageIndex(i, 0, 1, 0, 1);
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+numPages +" pages to SSDLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=1;i<=numPages;++i){
//			SSD.readPage(i);
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numPages +" pages from SSDLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		System.out.println("------------------------------------------------------");
//	}

}