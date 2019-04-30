package blockstorage;

public class ReadFromSSD implements Runnable {
	private Cache cache;
	SSD SSD;
	BlockServer server;

	ReadFromSSD(Cache cache, SSD SSD, BlockServer server){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
	}
	private void doWork() throws InterruptedException{
		if(server.readFromSSDQueue.size() > 0){
			int pageNumber = server.readFromSSDQueue.remove();
			if(pageNumber == -1){return;}
			if(SSD.pointersList.contains(pageNumber)){
				Page returnPage = SSD.readPage(pageNumber);
				boolean written = cache.writePage(returnPage,server);
				if(written){
					server.pageIndex.updatePageIndex(pageNumber, 1, -1, -1, -1);
//					System.out.println("Page: "+pageNumber+" written to cache from ReadFromSSDThread");
//					System.out.flush();
					server.debugLog("cache,2,"+pageNumber+",Page: " + pageNumber + " written to cache by ReadFromSSDThread");
				}
				else{
					server.debugLog("Error in writing Page: "+pageNumber+" to cache in ReadFromSSDThread");
					System.out.println("ERROR in writing Page: "+pageNumber+" to cache in ReadFromSSDThread");
				}
				server.readOutputQueue.add(returnPage);
			}
			else{
				server.readFromHDFSQueue.add(pageNumber);
			}
		}
		else{
			Thread.sleep(10);
		}

	}

	public void run(){
		while (true) {
			try {
				doWork();
			} catch (InterruptedException e) {
				System.out.println("InterruptedException in readFromSSDThread: " + e);
			}

			if(server.readFromSSDStop){
				if(server.readFromSSDQueue.size() == 0)
				{
					break;
				}
			}
		}
		System.out.println("readFromSSDThread ended.");
	}
}
