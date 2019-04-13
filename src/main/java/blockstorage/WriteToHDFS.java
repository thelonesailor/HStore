package blockstorage;

import java.io.FileInputStream;
import java.io.IOException;

public class WriteToHDFS implements Runnable {
	private Cache cache;
	private SSD SSD;
	private HDFSLayer HDFSlayer;
	private BlockServer server;
	private Utils utils;
	String SSD_LOCATION;


	WriteToHDFS(Cache cache, SSD SSD, HDFSLayer HDFSlayer, BlockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.HDFSlayer = HDFSlayer;
		this.server = server;
		this.utils = utils;
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}

	synchronized void doWork() throws InterruptedException{
		if(SSD.writeToHDFSQueue.size() > 0) {
			int pageNumber = SSD.writeToHDFSQueue.remove();
//			if(!server.pageIndex.get(pageNumber).isLocationSSD()){
//                System.out.println("Page "+pageNumber+" not in SSD.");
//				return;
//			}
//			System.out.println(pageNumber);
			String fileName = SSD_LOCATION + "/" + pageNumber;
//			File file = new File(fileName);
			try {
				SSD.pointersListLock.lock();
				if(SSD.pointersList.contains(pageNumber) && server.pageIndex.get(pageNumber).isDirty()) {
					FileInputStream in = new FileInputStream(fileName);
//	    			System.out.println("Writing "+pageNumber+" to HDFS.");
					byte[] pageData = new byte[utils.PAGE_SIZE];
					in.read(pageData);
					in.close();
					Page page = new Page(pageNumber, pageData);

					HDFSlayer.writePage(page, server);
					server.pageIndex.updatePageIndex(pageNumber, -1, 0, 1, -1);
					if(utils.SHOW_LOG)
						System.out.println("Page: "+pageNumber+" written to HDFSLayer");
				}
				else{
					server.pageIndex.updatePageIndex(pageNumber, -1, 0, -1, -1);
				}
//				file.delete();
				SSD.pointersList.remove((Integer)pageNumber);
				SSD.pointersListLock.unlock();
				if(utils.SHOW_LOG)
					System.out.println("Page: "+pageNumber+" removed from SSD");

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
//			System.out.println("writeToHDFSQueue.size()="+SSD.writeToHDFSQueue.size()+"  SSD.pointersList.size()="+SSD.pointersList.size()+"  SSD.recencylist.size="+SSD.recencyList.size()+" server.writeToHDFSStop="+server.writeToHDFSStop);


			if(server.writeToHDFSStop){
				if(!server.removeFromSSDThread.isAlive() && SSD.writeToHDFSQueue.size() == 0){
					break;
				}
			}
		}
		System.out.println("Write to HDFS thread ended.");
	}
}
