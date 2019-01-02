package BlockStorage;

import org.junit.Test;
import java.util.*;
import java.io.IOException;

import static org.junit.Assert.*;


public class blockServerTest {
	private Utils utils = new Utils();

	@Test
	public void WriteAndReadFromBlockServer1(){
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 1000;
		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");

		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=1; i<=numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			server.readPage(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		System.out.println("------------------------------------------------------");
	}

	@Test
	public void WriteAndReadFromBlockServer2(){
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 10000;
		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");

		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=1; i<=numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=1;i<=numPages;++i){
			server.readPage(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

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
		HDFSLayer HDFSLayer = new HDFSLayer();
		SSD SSD = new SSD(HDFSLayer);
		cache cache = new cache(SSD);

		int numPages = 10000;
		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Block Server made");

		double startTime = System.nanoTime();
		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=1; i<=numPages; ++i) {
			server.writePage(i, b);
		}
		double endTime = System.nanoTime();
		double time=(endTime - startTime) / 1000000000L;
		System.out.println("Wrote "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

		startTime = System.nanoTime();
		for(int i=numPages;i>=1;--i){
			server.readPage(i);
		}
		endTime = System.nanoTime();
		time=(endTime - startTime) / 1000000000L;
		System.out.println("Read "+numPages +" pages in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");

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