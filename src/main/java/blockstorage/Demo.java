package blockstorage;

import java.util.Scanner;

public class Demo {
	public static void main(String[] args){
		System.out.println("Main of Demo Running");
		Utils utils = new Utils(14, 16, 2, true);
		HDFSLayer HDFSLayer = new HDFSLayer(utils);
		SSD SSD = new SSD(HDFSLayer, utils);
		Cache cache = new Cache(SSD, utils);
		BlockServer server = new BlockServer(cache, SSD, HDFSLayer, utils);

		server.vMmanager.registerVM(1000);

		byte[] b = new byte[utils.PAGE_SIZE];
		for (int i=0; i<utils.CACHE_SIZE + utils.SSD_SIZE + utils.HDFS_BUFFER_SIZE*utils.BLOCK_SIZE; ++i) {
			server.writePage(i, b);
			server.printBlockServerStatus();
		}
		try{Thread.sleep(100);}
		catch(InterruptedException e){}
		server.stabilize();

		Scanner input = new Scanner(System.in);
		while(true){
			try{Thread.sleep(200);}
			catch(InterruptedException e){}
			server.stabilize();
			server.printBlockServerStatus();

			server.pageIndex.writeToFilePageIndex();
			server.writeToFileBlockServerStatus();

			try{Thread.sleep(1000);}
			catch(InterruptedException e){}
			server.debugLog("-,-,-,-");
			System.out.println("enter input:");
			String op = input.next();

			if(op.contentEquals("r")){
				int pageNumber = input.nextInt();
				System.out.println(op+" "+pageNumber);

				server.readPage(pageNumber);
			}
			else if(op.contentEquals("w")){
				int pageNumber = input.nextInt();
				System.out.println(op+" "+pageNumber);

				server.writePage(pageNumber, b);
			}
			else if(op.contentEquals("ws")){
				int pageNumber = input.nextInt();
				System.out.println(op+" "+pageNumber);

				server.writePageSynchronously(pageNumber, b);
			}
			else if(op.contentEquals("p")){
				server.printBlockServerStatus();
			}
			else if(op.contentEquals("reg")){
				int numberOfPages = input.nextInt();
				System.out.println(op+" "+numberOfPages);
				int VMID = server.vMmanager.registerVM(numberOfPages);
				System.out.println("VMID= "+VMID);
				int start = (VMID<<25);
				int end = start + numberOfPages - 1;
				System.out.println("Your range of page numbers is ["+start+","+end+"]");
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
