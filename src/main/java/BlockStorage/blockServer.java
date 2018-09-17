package BlockStorage;

import java.util.HashMap;

public class blockServer{
	
	cache cache;
	SSD SSD;
	HDFSLayer HDFSLayer;
	HashMap<Long, position> pageIndex;

	public blockServer(cache cache, SSD SSD, HDFSLayer HDFSLayer){
		pageIndex = new HashMap<Long, position>();
		this.cache = cache;
		this.SSD = SSD;
		this.HDFSLayer = HDFSLayer;
		pageIndex.clear();
	}

	public void createImage(){

	}

	public void deleteImage(){

	}
	/**
	 * @param pageNumber is 1 indexed
	 * */
	public page readPage(long pageNumber){
		page returnPage = null;
		position pos = pageIndex.get(pageNumber);

		if(pos.isLocationCache())
		{
			returnPage =  cache.readPage(pageNumber);
		}
		else if(pos.isLocationSSD())
		{
			returnPage = SSD.readPage(pageNumber);
			cache.writePage(returnPage, false, this);
			updatePageIndex(pageNumber, true, true, pos.isLocationHDFS());
		}
		else if(pos.isLocationHDFS()){
			block returnBlock = HDFSLayer.readBlock(pageNumber);
			returnPage = returnBlock.readPage(pageNumber);
			page[] returnAllPages = returnBlock.getAllPages();
			for (int i = 0; i < 8; i++){
				// TODO : if condition to be added to check the validity
				position p = pageIndex.get(returnBlock.blockNumber+i);
				if(p!=null && p.isLocationHDFS() && cache.cacheList.get(returnBlock.blockNumber+i)==null) {
					cache.writePage(returnAllPages[i], false, this);
					updatePageIndex(pageNumber, true, false, true);
				}
			}
		}else {

		}
		return returnPage;
	}

	/**
	 * @param pageNumber is 1 indexed
	 * */
	void writePage(long pageNumber, byte[] pageData){
		page newPage = new page(pageNumber, pageData);
		cache.writePage(newPage, true, this);
		updatePageIndex(pageNumber, true, false, false);
	}

	public void shutdown(){

	}

	public void recover(){

	}

	public void addPageIndexEntry(long pageNumber,boolean locationCache){
		position newEntry = new position(locationCache, false, false);
		pageIndex.put(pageNumber, newEntry);
	}

	public void updatePageIndex(long pageNumber,boolean locationCache, boolean locationSSD, boolean locationHDFS){
		pageIndex.put(pageNumber, new position(locationCache, locationSSD, locationHDFS));
	}

}