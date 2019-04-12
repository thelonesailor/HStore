package blockstorage;

public class HDFSLayerTest {
//	private Utils utils = new Utils();

//	@Test
//	public void WriteAndReadFromHDFSClusterDirectly1() {
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		Cache Cache = new Cache(SSD);
//
//		int numPages = 100;
//		int numBlocks = numPages>>3;
//		BlockServer server = new BlockServer(Cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//
//		FileSystemOperations client = new FileSystemOperations();
//		double startTime = System.nanoTime();
//
//		for(int i=1;i<=numBlocks;++i){
//			Block Block = new Block(i, new byte[8 * utils.PAGE_SIZE]);
//			try {
//				client.addFile(HDFSLayer.config, Block);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+ numBlocks +" blocks to HDFS directly in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=1;i<=numBlocks;++i){
//			try {
//				client.readFile(HDFSLayer.config, i);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numBlocks +" blocks from HDFS directly in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		server.stop();
//		System.out.println("------------------------------------------------------");
//	}
//
//	@Test
//	public void WriteAndReadFromHDFSClusterDirectly2() {
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		Cache Cache = new Cache(SSD);
//
//		int numPages = 1000;
//		int numBlocks = numPages>>3;
//		BlockServer server = new BlockServer(Cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//
//		FileSystemOperations client = new FileSystemOperations();
//		double startTime = System.nanoTime();
//
//		for(int i=1;i<=numBlocks;++i){
//			Block Block = new Block(i, new byte[8 * utils.PAGE_SIZE]);
//			try {
//				client.addFile(HDFSLayer.config, Block);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+ numBlocks +" blocks to HDFS directly in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=1;i<=numBlocks;++i){
//			try {
//				client.readFile(HDFSLayer.config, i);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numBlocks +" blocks from HDFS directly in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		server.stop();
//		System.out.println("------------------------------------------------------");
//	}
//
//	@Test
//	public void WriteAndReadFromHDFSLayer1() {
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		Cache Cache = new Cache(SSD);
//
//		int numPages = 1000;
//
//		BlockServer server = new BlockServer(Cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//		byte[] b = new byte[utils.PAGE_SIZE];
//		double startTime = System.nanoTime();
//		for(int i=1;i<=numPages;++i){
//			HDFSLayer.writePage(new Page(i, b), server);
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=1;i<=numPages;++i){
//			HDFSLayer.readBlock(i, server);
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		server.stop();
//		System.out.println("------------------------------------------------------");
//	}
//
//	@Test
//	public void WriteAndReadFromHDFSLayer2() {
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		Cache Cache = new Cache(SSD);
//
//		int numPages = 10000;
//
//		BlockServer server = new BlockServer(Cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//		byte[] b = new byte[utils.PAGE_SIZE];
//		double startTime = System.nanoTime();
//		for(int i=1;i<=numPages;++i){
//			HDFSLayer.writePage(new Page(i, b), server);
//			server.updatePageIndex(i, 0, 0, 1, 1);
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=1;i<=numPages;++i){
//			HDFSLayer.readBlock(i, server);
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		server.stop();
//		System.out.println("------------------------------------------------------");
//	}
//
//	@Test
//	public void WriteAndRevReadFromHDFSLayer1() {
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		Cache Cache = new Cache(SSD);
//
//		int numPages = 1000;
//
//		BlockServer server = new BlockServer(Cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//		byte[] b = new byte[utils.PAGE_SIZE];
//		double startTime = System.nanoTime();
//		for(int i=0;i<numPages;++i){
//			HDFSLayer.writePage(new Page(i, b), server);
//			server.updatePageIndex(i, 0, 0, 1, 1);
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=(numPages>>3);i>=0;--i){
//			HDFSLayer.readBlock(i, server);
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		server.stop();
//		System.out.println("------------------------------------------------------");
//	}
//
//	@Test
//	public void WriteAndRevReadFromHDFSLayer2() {
//		HDFSLayer HDFSLayer = new HDFSLayer();
//		SSD SSD = new SSD(HDFSLayer);
//		Cache Cache = new Cache(SSD);
//
//		int numPages = 10000;
//
//		BlockServer server = new BlockServer(Cache, SSD, HDFSLayer);
//		System.out.println("Block Server made");
//		byte[] b = new byte[utils.PAGE_SIZE];
//		double startTime = System.nanoTime();
//		for(int i=0;i<numPages;++i){
//			HDFSLayer.writePage(new Page(i, b), server);
//			server.updatePageIndex(i, 0, 0, 1, 1);
//		}
//		double endTime = System.nanoTime();
//		double time=(endTime - startTime) / 1000000000L;
//		System.out.println("Wrote "+numPages +" pages to HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		startTime = System.nanoTime();
//		for(int i=(numPages>>3);i>=0;--i){
//			HDFSLayer.readBlock(i, server);
//		}
//		endTime = System.nanoTime();
//		time=(endTime - startTime) / 1000000000L;
//		System.out.println("Read "+numPages +" pages from HDFSLayer in "+time+" seconds at "+(16*numPages*1.0)/time+"kB/s");
//
//		server.stop();
//		System.out.println("------------------------------------------------------");
//	}

}