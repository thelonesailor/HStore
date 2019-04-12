package blockstorage;

import java.util.Scanner;

public class Demo {
	public static void main(String[] args){
		System.out.println("Main Running");
		Utils utils = new Utils(14, 18, 2, true);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<utils.CACHE_SIZE + utils.SSD_SIZE + utils.HDFS_BUFFER_SIZE*utils.BLOCK_SIZE; ++i) {
			server.writePage(i, b);
			server.printBlockServerStatus();
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		Scanner input = new Scanner(System.in);
		while(true){
			System.out.println("enter input:");
			String op = input.next();

			if(op.contentEquals("r")){
				int pageNumber = input.nextInt();
				System.out.println(op+" "+pageNumber);

				server.readPage(pageNumber);
				server.printBlockServerStatus();
			}
			else if(op.contentEquals("w")){
				int pageNumber = input.nextInt();
				System.out.println(op+" "+pageNumber);

				server.writePage(pageNumber, b);
				server.printBlockServerStatus();
			}
			else if(op.contentEquals("e")){
				break;
			}
			else{}
		}

		server.normalShutdown();
		System.out.println("------------------------------------------------------");
	}
}
