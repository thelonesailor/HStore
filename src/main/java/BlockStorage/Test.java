package BlockStorage;

// import java.util.*;
// import java.io.IOException;

public class Test{
	HDFSLayer HDFSLayer = new HDFSLayer();
	SSD SSD = new SSD(HDFSLayer);
	cache cache = new cache(SSD);
	Utils utils = new Utils();

	public void shit(){
		FileSystemOperations client = new FileSystemOperations();
    // 	block block = new block(0,new byte[8*utils.PAGE_SIZE]);
    // 	try{
    // 		client.deleteFile(0,HDFSLayer.config);
    // 	client.addFile(HDFSLayer.config,block);
    // }
    // catch(IOException e){
    // 	e.printStackTrace();
    // }
		

		blockServer server = new blockServer(cache, SSD, HDFSLayer);
		System.out.println("Shit done");
		long startTime = System.nanoTime();
		runSeqTest(110000,server);
		long endTime = System.nanoTime();
		System.out.println((endTime-startTime));
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

	public void runSeqTest(long size,blockServer server){
		
		
		byte[] b = new byte[utils.PAGE_SIZE];
		
		// long commitCycle = (30<<20)/BlockProtocol.clientBlockSize;
		
		// (size<<10)/utils.PAGE_SIZE
		for (int i=0; i<size; i++){
//		for (int i=0; i<40; i++){
			
//			System.out.println("addr: " + blockData.addr);
			
			server.writePage((long)i,b);
		}
//		hbs.commit(imageKey);
		
	}
}