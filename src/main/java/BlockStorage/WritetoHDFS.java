package BlockStorage;

import java.io.FileInputStream;
import java.io.IOException;

public class WritetoHDFS implements Runnable {
	private cache cache;
	private SSD SSD;
	private HDFSLayer HDFSlayer;
	private blockServer server;
	private Utils utils;
	String SSD_LOCATION;


	WritetoHDFS(cache cache,SSD SSD,HDFSLayer HDFSlayer, blockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.HDFSlayer = HDFSlayer;
		this.server = server;
		this.utils = utils;
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}

	synchronized void doWork() throws InterruptedException{
		if(SSD.WritetoHDFSqueue.size() > 0) {
			Long p = SSD.WritetoHDFSqueue.remove();
			long pageNumber = p;
//			if(!server.pageIndex.get(pageNumber).isLocationSSD()){
//                System.out.println("page "+pageNumber+" not in SSD.");
//				return;
//			}
//			System.out.println(pageNumber);
			String fileName = SSD_LOCATION + "/" + pageNumber;
//			File file = new File(fileName);
			try {

				if(SSD.pointersList.contains(pageNumber) && server.pageIndex.get(pageNumber).isDirty()) {
					FileInputStream in = new FileInputStream(fileName);
//	    			System.out.println("Writing "+pageNumber+" to HDFS.");
					byte[] pageData = new byte[utils.PAGE_SIZE];
					in.read(pageData);
					in.close();
					page page = new page(pageNumber, pageData);

					HDFSlayer.writePage(page, server);
					server.updatePageIndex(pageNumber, -1, 0, 1, -1);
					if(utils.SHOW_LOG)
						System.out.println("page: "+pageNumber+" written to HDFSLayer");
				}
				else{
					server.updatePageIndex(pageNumber, -1, 0, -1, -1);
				}
//				file.delete();
				SSD.pointersList.remove(pageNumber);
				if(utils.SHOW_LOG)
					System.out.println("page: "+pageNumber+" removed from SSD");

			} catch (IOException e) {
				System.out.println("Exception Occurred:");
				e.printStackTrace();
			}
		}
		else{
			Thread.sleep(10);
		}
	}

	public void run(){

		while (true) {
			try{
//				server.Lock2.lock();
				doWork();
//				server.Lock2.unlock();
			}
			catch(InterruptedException e){
				System.out.println("InterruptedException in Write to HDFS thread: " + e);
			}
//			System.out.println("WritetoHDFSqueue.size()="+SSD.WritetoHDFSqueue.size()+"  SSD.pointersList.size()="+SSD.pointersList.size()+"  SSD.recencylist.size="+SSD.recencyList.size()+" server.writetoHDFSStop="+server.writetoHDFSStop);


			if(server.writetoHDFSStop){
				if(!server.removeFromSSDthread.isAlive() && SSD.WritetoHDFSqueue.size() == 0){
					break;
				}
			}
		}
		System.out.println("Write to HDFS thread ended.");
	}
}
