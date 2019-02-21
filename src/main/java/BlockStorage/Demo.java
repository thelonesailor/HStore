package BlockStorage;

import java.util.Scanner;

public class Demo {
	public static void main(String args[]){
		System.out.println("Main Running");
		Utils utils = new Utils(14, 18, 2, true);
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

		Scanner input = new Scanner(System.in);
		System.out.println("enter input");
		while(true){
			String op = input.next();
			int pageNumber = input.nextInt();
			System.out.println(op+" "+pageNumber);

			if(op.contentEquals("r")){
				System.out.println("reading");
				server.readPage(pageNumber);
				server.printBlockServerStatus();
			}
			else if(op.contentEquals("w")){
				server.writePage(pageNumber, b);
				server.printBlockServerStatus();
			}
			else if(op.contentEquals("e")){
				break;
			}
			else{}
		}

		server.stop();
		System.out.println("------------------------------------------------------");
	}
}
