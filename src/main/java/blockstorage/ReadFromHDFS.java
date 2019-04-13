package blockstorage;

public class ReadFromHDFS implements Runnable {
	private Cache cache;
	SSD SSD;
	HDFSLayer HDFSLayer;
	BlockServer server;
	private Utils utils;

	ReadFromHDFS(Cache cache, SSD SSD, HDFSLayer HDFSLayer, BlockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.HDFSLayer = HDFSLayer;
		this.utils = utils;
	}
	private void doWork() throws InterruptedException{
		if(server.readFromHDFSQueue.size() > 0){
			int pageNumber = server.readFromHDFSQueue.remove();

			if(utils.SHOW_LOG)
				System.out.println("Page " + pageNumber + " removed from readFromHDFSQueue");

			Block returnBlock = HDFSLayer.readBlock(pageNumber, server);
			Page returnPage = returnBlock.readPage(pageNumber);

			Page[] returnAllPages = returnBlock.getAllPages();
			for (int i = 0; i < utils.BLOCK_SIZE; i++){
				int temp = ((returnBlock.blockNumber)<<3)+i;
				Position p = server.pageIndex.get(temp);
				if(p!=null && p.isLocationHDFS() && !p.isDirty() && !p.isLocationCache() && cache.pointersList.get(temp)==null) {
					cache.writePage(returnAllPages[i],server);
				if(utils.SHOW_LOG)
					System.out.println("Page " + temp + " written to cache");

					server.pageIndex.updatePageIndex(temp, 1, -1, 1, -1);
				}
			}
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
				System.out.println("InterruptedException in readFromHDFSThread: " + e);
			}

			if(server.readFromHDFSStop){
				if(server.readFromHDFSQueue.size() == 0)
				{
					break;
				}
			}
		}
		System.out.println("readFromHDFSThread ended.");
	}
}
