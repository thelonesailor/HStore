package BlockStorage;

import java.util.HashMap;

class blockServer{
	
	cache cache;
	SSD SSD;
	HDFSLayer HDFSLayer;
	HashMap<Long, position> pageIndex;
	private Utils utils = new Utils();

	public blockServer(cache cache, SSD SSD, HDFSLayer HDFSLayer){
		pageIndex = new HashMap<Long, position>();
		this.cache = cache;
		this.SSD = SSD;
		this.HDFSLayer = HDFSLayer;
		pageIndex.clear();
	}


	/**
	 * @param pageNumber is 1 indexed
	 * */
	page readPage(long pageNumber){
		page returnPage = null;
		position pos = pageIndex.get(pageNumber);

		if(pos.isLocationCache())
		{
			returnPage =  cache.readPage(pageNumber);
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
				// TODO : if condition to be added to check the validity
				long temp = ((returnBlock.blockNumber)<<3)+i;
				position p = pageIndex.get(temp);
				if(p!=null && p.isLocationHDFS() && !p.isDirty() && !p.isLocationCache() && cache.cacheList.get(temp)==null) {
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
	 * @param pageNumber is 1 indexed
	 * */
	void writePage(long pageNumber, byte[] pageData){
		page newPage = new page(pageNumber, pageData);
		cache.writePage(newPage, this);
		updatePageIndex(pageNumber, 1, 0, 0, 1);
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