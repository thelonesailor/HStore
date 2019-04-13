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
			Page returnPage = SSD.readPage(pageNumber);
			cache.writePage(returnPage,server);
			server.pageIndex.updatePageIndex(pageNumber, 1, -1, -1, -1);
			server.readOutputQueue.add(returnPage);
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
