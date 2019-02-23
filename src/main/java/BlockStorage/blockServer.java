package BlockStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class blockServer{
	
	cache cache;
	SSD SSD;
	HDFSLayer HDFSLayer;
	ConcurrentHashMap<Long, position> pageIndex;
	private Utils utils;

	RemoveFromCache removeFromCache;
	WritetoSSD writetoSSD;
	RemoveFromSSD removeFromSSD;
	WritetoHDFS writetoHDFS;

	Thread removeFromCachethread;
	Thread writeToSSDthread;
	Thread removeFromSSDthread;
	Thread writeToHDFSthread;

	Lock Lock1 = new ReentrantLock();
	Lock Lock2 = new ReentrantLock();

	boolean removeFromCacheStop = false;
	boolean writetoSSDStop = false;
	boolean removeFromSSDStop = false;
	boolean writetoHDFSStop = false;

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
		System.out.println("removeFromCachethread started.");
		removeFromCachethread.setName("removeFromCachethread");

		this.writetoSSD = new WritetoSSD(this.cache, this.SSD, this, this.utils);
		writeToSSDthread = new Thread(this.writetoSSD);
		writeToSSDthread.start();
		System.out.println("writetoSSDthread started.");
		writeToSSDthread.setName("writetoSSDthread");

		this.removeFromSSD = new RemoveFromSSD(this.cache, this.SSD, this, this.utils);
		removeFromSSDthread = new Thread(this.removeFromSSD);
		removeFromSSDthread.start();
		System.out.println("removeFromSSDthread started.");
		removeFromSSDthread.setName("removeFromSSDthread");

		this.writetoHDFS = new WritetoHDFS(this.cache, this.SSD, this.HDFSLayer, this, this.utils);
		writeToHDFSthread = new Thread(this.writetoHDFS);
		writeToHDFSthread.start();
		System.out.println("writeToHDFSthread started.");
		writeToHDFSthread.setName("writetoHDFSthread");

		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		System.out.println("blockServer initialised");
	}


	void recover() throws IOException {

		FileSystemOperations client = new FileSystemOperations(this.utils);
		Configuration config = client.getConfiguration();
		FileSystem fileSystem = FileSystem.get(config);


		Path HDFSpath = new Path(config.get("fs.defaultFS")+utils.HDFS_PATH+"/");
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(HDFSpath, false);

		System.out.println("Pages found in HDFS Cluster: ");
		String s = "";

		while(fileStatusListIterator.hasNext()){
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			//do stuff with the file like ...
//			System.out.println(fileStatus.getPath());
			String str = fileStatus.getPath().toString();
			String[] arrOfStr = str.split("/");
			long blockNumber = (long)Integer.parseInt(arrOfStr[arrOfStr.length-1]);

//			System.out.println(blockNumber);


			HDFSLayer.blockList.put(blockNumber, true);

			for(int i=0;i<utils.BLOCK_SIZE;++i){
				long pageNumber = (blockNumber<<3) + i;
				updatePageIndex(pageNumber, 0,0,1,0);
				s += pageNumber+"  ";
			}
		}
		System.out.println(s);


		File SSDdirectory = new File(utils.SSD_LOCATION+"/");
		String[] paths = SSDdirectory.list();
		s = "Pages found in SSD:\n";
		for(String path:paths) {
			long pageNumber = (long)Integer.parseInt(path);
			SSD.pointersList.add(pageNumber);
			SSD.recencyList.put(pageNumber, true);
//			System.out.println(pageNumber);
			s += pageNumber+"  ";
			updatePageIndex(pageNumber, 0,1,0,1);

			if(SSD.pointersList.size() >= utils.SSD_SIZE){
				System.out.println("ERROR max SSD size reached");
				break;
			}
		}
		System.out.println(s);

	}


	void stop(){
		stablize();
		removeFromCacheStop = true;
		writetoSSDStop = true;
		removeFromSSDStop = true;
		writetoHDFSStop = true;

		System.out.println("Stop function called, threads signalled to stop, waiting for threads to join");
		try{
			removeFromCachethread.join();
			writeToSSDthread.join();
			removeFromSSDthread.join();
			writeToHDFSthread.join();
		}
		catch(InterruptedException e){
			System.out.println("InterruptedException in joining: " + e);
		}

		File SSDdirectory = new File(utils.SSD_LOCATION+"/");
		String[] paths = SSDdirectory.list();
		for(String path:paths) {
			String fileName = utils.SSD_LOCATION + "/" + path;
			File file = new File(fileName);
			file.delete();
		}

		HDFSLayer.flushHDFSBuffer();
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
			updatePageIndex(pageNumber, 1, -1, -1, -1);
		}
		else if(pos.isLocationHDFS()){
			if(utils.SHOW_LOG)
				System.out.println("Reading page " + pageNumber + " from HDFS Layer");

//			printBlockServerStatus();

			block returnBlock = HDFSLayer.readBlock(pageNumber, this);
			returnPage = returnBlock.readPage(pageNumber);

			page[] returnAllPages = returnBlock.getAllPages();
			for (int i = 0; i < utils.BLOCK_SIZE; i++){
				// if condition to be added to check the validity
				long temp = ((returnBlock.blockNumber)<<3)+(long)i;
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

		if(written){
			updatePageIndex(pageNumber, 1, 0, 0, 1);
			if(utils.SHOW_LOG)
				System.out.println("page: "+pageNumber+" written to BlockServer");
		}
		else{
			System.out.println("Error in writing page to cache");
		}
	}


	synchronized void updatePageIndex(long pageNumber,int Cache, int SSD, int HDFS, int dirty){
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
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
				assert false;
				return;
			}

			if (SSD == 1) locationSSD = true;
			else if (SSD == 0) locationSSD = false;
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
				assert false;
				return;
			}

			if (HDFS == 1) locationHDFS = true;
			else if (HDFS == 0) locationHDFS = false;
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
				assert false;
				return;
			}

			if (dirty == 1) dirtyBit = true;
			else if (dirty == 0) dirtyBit = false;
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
				assert false;
				return;
			}

			pageIndex.put(pageNumber, new position(locationCache, locationSSD, locationHDFS, dirtyBit));
		}
	}

	void stablize(){
//		while (cache.cacheList.size() > utils.MAX_CACHE_FULL_SIZE){}
		while (SSD.WritetoSSDqueue.size() > 0){}
		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}
//		while (SSD.recencyList.size() > utils.MAX_SSD_FULL_SIZE){}
		while (SSD.WritetoHDFSqueue.size() > 0){}
		while (SSD.pointersList.size() > utils.MAX_SSD_FULL_SIZE){}
	}

	void printBlockServerStatus(){
		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}
		while (SSD.pointersList.size() > utils.MAX_SSD_FULL_SIZE){}

		while (SSD.WritetoSSDqueue.size() > 0){}
		while (SSD.WritetoHDFSqueue.size() > 0){}

		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		System.out.println("Printing BlockServer status:");
		System.out.println("Pages in Cache["+utils.MAX_CACHE_FULL_SIZE+", "+utils.CACHE_SIZE+"]:");
		String s = "";
		for (long k: cache.pointersList.keySet()){
//			System.out.println(k);
			s += k+"  ";
		}
		System.out.println(s);

		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}
		while (SSD.pointersList.size() > utils.MAX_SSD_FULL_SIZE){}
		s = "";
		System.out.println("Pages in SSD["+utils.MAX_SSD_FULL_SIZE+", "+utils.SSD_SIZE+"]:");
		for (long k: SSD.pointersList){
//			System.out.println(k);
			s += k+"  ";
		}
		System.out.println(s);

		s = "";
		System.out.println("Pages in HDFS Buffer["+utils.HDFS_BUFFER_SIZE+" Blocks, " +utils.HDFS_BUFFER_SIZE*8+" Pages]:");
		for (long k: HDFSLayer.HDFSBufferList.keySet()){
			for (long i=0;i<utils.BLOCK_SIZE;++i){
				long pageNumber = (k<<3) + i;
				if(pageIndex.containsKey(pageNumber) && pageIndex.get(pageNumber).isLocationHDFS()){
//					System.out.println(pageNumber);
					s += pageNumber+" ";
				}
			}
			s+=" ";
		}
		System.out.println(s);

		s = "";
		System.out.println("Pages in HDFS Cluster:");
		for (long k: HDFSLayer.blockList.keySet()){
			for (long i=0;i<utils.BLOCK_SIZE;++i){
				long pageNumber = (k<<3) + i;
				if(pageIndex.containsKey(pageNumber) && pageIndex.get(pageNumber).isLocationHDFS()){
//					System.out.println(pageNumber);
					s += pageNumber+" ";
				}
			}
			s+=" ";
		}
		System.out.println(s);

		System.out.println("------------------------------");
	}
}