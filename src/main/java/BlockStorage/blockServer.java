package BlockStorage;

import java.util.concurrent.ConcurrentHashMap;

class blockServer{
	
	cache cache;
	SSD SSD;
	HDFSLayer HDFSLayer;
	ConcurrentHashMap<Long, position> pageIndex;
	private Utils utils;
	WritetoSSD writetoSSD;
	RemoveFromCache removeFromCache;

	Thread removeFromCachethread;
	Thread writetoSSDthread;

	Thread removeFromSSDthread;
	Thread writetoHDFSthread;

	boolean removeFromCacheStop = false;
	boolean writetoSSDStop = false;

	blockServer(cache cache, SSD SSD, HDFSLayer HDFSLayer, Utils utils){
		pageIndex = new ConcurrentHashMap<>();
		pageIndex.clear();
		this.cache = cache;
		this.SSD = SSD;
		this.HDFSLayer = HDFSLayer;
		this.utils = utils;

		this.removeFromCache = new RemoveFromCache(this.cache, this.SSD, this, this.utils);
		removeFromCachethread = new Thread(this.removeFromCache);
		removeFromCachethread.start();
		removeFromCachethread.setName("removeFromCachethread");

		this.writetoSSD = new WritetoSSD(this.cache, this.SSD, this, this.utils);
		writetoSSDthread = new Thread(this.writetoSSD);
		writetoSSDthread.start();
		writetoSSDthread.setName("writetoSSDthread");
	}

	void stop(){
		removeFromCacheStop = true;
		writetoSSDStop = true;

		System.out.println("Interrupts sent, waiting to join");
		try{
			removeFromCachethread.join();
			writetoSSDthread.join();
		}
		catch(InterruptedException e){
			System.out.println("InterruptedException in joining: " + e);
		}

		HDFSLayer.flushHDFSbuffer();
		HDFSLayer.closeFS();
	}

	/**
	 * @param pageNumber is 1 indexed
	 * */
	page readPage(long pageNumber){
		page returnPage = null;
		position pos = pageIndex.get(pageNumber);

		if(pos.isLocationCache())
		{
			returnPage =  cache.readPage(pageNumber, false);
		}
		else if(pos.isLocationSSD())
		{
			returnPage = SSD.readPage(pageNumber);
			cache.writePage(returnPage,this);
			updatePageIndex(pageNumber, 1, 1, -1, -1);
		}
		else if(pos.isLocationHDFS()){
			block returnBlock = HDFSLayer.readBlock(pageNumber, this);
			returnPage = returnBlock.readPage(pageNumber);

			page[] returnAllPages = returnBlock.getAllPages();
			for (int i = 0; i < utils.BLOCK_SIZE; i++){
				// if condition to be added to check the validity
				long temp = ((returnBlock.blockNumber)<<3)+i;
				position p = pageIndex.get(temp);
				if(p!=null && p.isLocationHDFS() && !p.isDirty() && !p.isLocationCache() && cache.pointersList.get(temp)==null) {
					cache.writePage(returnAllPages[i],this);
					updatePageIndex(temp, 1, -1, 1, -1);
				}
			}
		}else {
			System.out.println("Error finding page: "+pageNumber);
		}
		return returnPage;
	}

	/**
	 * @param pageNumber is 0 indexed
	 * */
	void writePage(long pageNumber, byte[] pageData){
		page newPage = new page(pageNumber, pageData);
		boolean written = cache.writePage(newPage, this);

		if(written)
		updatePageIndex(pageNumber, 1, 0, 0, 1);
		else{
			System.out.println("Error in writing page to cache");
		}
	}


	void updatePageIndex(long pageNumber,int Cache, int SSD, int HDFS, int dirty){
		boolean locationCache, locationSSD, locationHDFS, dirtyBit;
		if(pageIndex.containsKey(pageNumber)) {
			position po = pageIndex.get(pageNumber);

			if (Cache == 1) locationCache = true;
			else if (Cache == 0) locationCache = false;
			else locationCache = po.locationCache;

			if (SSD == 1) locationSSD = true;
			else if (SSD == 0) locationSSD = false;
			else locationSSD = po.locationSSD;

			if (HDFS == 1) locationHDFS = true;
			else if (HDFS == 0) locationHDFS = false;
			else locationHDFS = po.locationHDFS;

			if (dirty == 1) dirtyBit = true;
			else if (dirty == 0) dirtyBit = false;
			else dirtyBit = po.diryBit;

			pageIndex.remove(pageNumber);
			pageIndex.put(pageNumber, new position(locationCache, locationSSD, locationHDFS, dirtyBit));
		}
		else {

			if (Cache == 1) locationCache = true;
			else if (Cache == 0) locationCache = false;
			else {System.out.println("Error in updating pageIndex for "+pageNumber);return;}

			if (SSD == 1) locationSSD = true;
			else if (SSD == 0) locationSSD = false;
			else {System.out.println("Error in updating pageIndex for "+pageNumber);return;}

			if (HDFS == 1) locationHDFS = true;
			else if (HDFS == 0) locationHDFS = false;
			else {System.out.println("Error in updating pageIndex for "+pageNumber);return;}

			if (dirty == 1) dirtyBit = true;
			else if (dirty == 0) dirtyBit = false;
			else {System.out.println("Error in updating pageIndex for "+pageNumber);return;}

			pageIndex.put(pageNumber, new position(locationCache, locationSSD, locationHDFS, dirtyBit));
		}
	}
}