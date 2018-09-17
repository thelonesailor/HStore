package BlockStorage;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class HDFSLayerTest {
	private Utils utils = new Utils();

	@Test
	public void WriteAndReadFromHDFSDirectly1() {
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 1000;
		int numBlocks = numPages/8;
		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");

		FileSystemOperations client = new FileSystemOperations();
		double startTime = System.nanoTime();

		for(int i=1;i<=numBlocks;++i){
			block block = new block(i, new byte[8 * utils.PAGE_SIZE]);
			try {
				client.addFile(HDFSLayer.config, block);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=1;i<=numBlocks;++i){
			try {
				client.readFile(HDFSLayer.config, i);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromHDFSLayer1() {
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 1000;

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		byte[] b = new byte[utils.PAGE_SIZE];
		double startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			HDFSLayer.writePage(new page(i, b), server);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			HDFSLayer.readBlock(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromHDFSLayer2() {
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 10000;

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		byte[] b = new byte[utils.PAGE_SIZE];
		double startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			HDFSLayer.writePage(new page(i, b), server);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			HDFSLayer.readBlock(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndRevReadFromHDFSLayer1() {
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 1000;

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		byte[] b = new byte[utils.PAGE_SIZE];
		double startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			HDFSLayer.writePage(new page(i, b), server);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=numPages;i>=1;--i){
			HDFSLayer.readBlock(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndRevReadFromHDFSLayer2() {
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 10000;

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");
		byte[] b = new byte[utils.PAGE_SIZE];
		double startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			HDFSLayer.writePage(new page(i, b), server);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=numPages;i>=1;--i){
			HDFSLayer.readBlock(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

}