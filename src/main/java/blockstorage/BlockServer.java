package blockstorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class BlockServer {
	
	Cache cache;
	SSD SSD;
	HDFSLayer HDFSLayer;
	private Utils utils;
	PageIndex pageIndex;
	VMManager vMmanager;

	@NotNull ConcurrentLinkedQueue<Integer> readFromSSDQueue = new ConcurrentLinkedQueue<>();
	@NotNull ConcurrentLinkedQueue<Integer> readFromHDFSQueue = new ConcurrentLinkedQueue<>();
	@NotNull ConcurrentLinkedQueue<Page> readOutputQueue = new ConcurrentLinkedQueue<>();

	RemoveFromCache removeFromCache;
	WriteToSSD writeToSSD;
	RemoveFromSSD removeFromSSD;
	WriteToHDFS writeToHDFS;
	ReadFromSSD readFromSSD;
	ReadFromHDFS readFromHDFS;

	Thread removeFromCacheThread;
	Thread writeToSSDThread;
	Thread removeFromSSDThread;
	Thread writeToHDFSThread;
	Thread readFromSSDThread;
	Thread readFromHDFSThread;

	@NotNull Lock Lock1 = new ReentrantLock();
	@NotNull Lock Lock2 = new ReentrantLock();

	boolean removeFromCacheStop = false;
	boolean writeToSSDStop = false;
	boolean removeFromSSDStop = false;
	boolean writeToHDFSStop = false;
	boolean readFromSSDStop = false;
	boolean readFromHDFSStop = false;

	BlockServer(Cache cache, SSD SSD, HDFSLayer HDFSLayer, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.HDFSLayer = HDFSLayer;
		this.utils = utils;
		this.pageIndex = new PageIndex();
		this.vMmanager = new VMManager(this);

		this.removeFromCache = new RemoveFromCache(this.cache, this.SSD, this, this.utils);
		removeFromCacheThread = new Thread(this.removeFromCache);
		removeFromCacheThread.start();
		System.out.println("removeFromCacheThread started.");
		removeFromCacheThread.setName("removeFromCacheThread");

		this.writeToSSD = new WriteToSSD(this.cache, this.SSD, this, this.utils);
		writeToSSDThread = new Thread(this.writeToSSD);
		writeToSSDThread.start();
		System.out.println("writetoSSDthread started.");
		writeToSSDThread.setName("writetoSSDthread");

		this.removeFromSSD = new RemoveFromSSD(this.cache, this.SSD, this, this.utils);
		removeFromSSDThread = new Thread(this.removeFromSSD);
		removeFromSSDThread.start();
		System.out.println("removeFromSSDThread started.");
		removeFromSSDThread.setName("removeFromSSDThread");

		this.writeToHDFS = new WriteToHDFS(this.cache, this.SSD, this.HDFSLayer, this, this.utils);
		writeToHDFSThread = new Thread(this.writeToHDFS);
		writeToHDFSThread.start();
		System.out.println("writeToHDFSThread started.");
		writeToHDFSThread.setName("writetoHDFSthread");

		this.readFromSSD = new ReadFromSSD(this.cache, this.SSD, this);
		readFromSSDThread = new Thread(this.readFromSSD);
		readFromSSDThread.start();
		System.out.println("readFromSSDThread started.");
		readFromSSDThread.setName("readFromSSDThread");

		this.readFromHDFS = new ReadFromHDFS(this.cache, this.SSD, this.HDFSLayer, this, utils);
		readFromHDFSThread = new Thread(this.readFromHDFS);
		readFromHDFSThread.start();
		System.out.println("readFromHDFSThread started.");
		readFromHDFSThread.setName("readFromHDFSThread");

		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		System.out.println("BlockServer initialised");
	}


	void recover() throws IOException {
		FileSystemOperations client = new FileSystemOperations(this.utils);
		Configuration config = client.getConfiguration();
		FileSystem fileSystem = FileSystem.get(config);

		System.out.println("--------------------------------------------------");
		File SSDdirectory = new File(utils.SSD_LOCATION+"/");
		String[] paths = SSDdirectory.list();
		String s = "Pages found in SSD:\n";
		for(String path:paths) {
			int pageNumber = Integer.parseInt(path);
			SSD.pointersList.add(pageNumber);
			SSD.recencyList.put(pageNumber, true);
//			System.out.println(pageNumber);
			s += pageNumber+"  ";
			pageIndex.updatePageIndex(pageNumber, 0,1,0,1);
			if(SSD.pointersList.size() >= utils.SSD_SIZE){
				System.out.println("ERROR max SSD size reached");
				break;
			}
		}
		System.out.println(s);


		Path HDFSpath = new Path(config.get("fs.defaultFS")+utils.HDFS_PATH+"/");
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(HDFSpath, false);
		System.out.println("Pages found in HDFS Cluster: ");
		s = "";
		while(fileStatusListIterator.hasNext()){
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			//do stuff with the file like ...
//			System.out.println(fileStatus.getPath());
			String str = fileStatus.getPath().toString();
			String[] arrOfStr = str.split("/");
			int blockNumber = Integer.parseInt(arrOfStr[arrOfStr.length-1]);
//			System.out.println(blockNumber);

			HDFSLayer.blockList.put(blockNumber, true);
			for(int i=0;i<utils.BLOCK_SIZE;++i){
				int pageNumber = (blockNumber<<3) + i;
				pageIndex.updatePageIndex(pageNumber, 0,0,1,0);
				s += pageNumber+"  ";
			}
		}
		System.out.println(s);
		System.out.println("--------------------------------------------------");
	}


	void normalShutdown(){
		stabilize();
		removeFromCacheStop = true;
		writeToSSDStop = true;
		removeFromSSDStop = true;
		writeToHDFSStop = true;
		readFromSSDStop = true;
		readFromHDFSStop = true;

		System.out.println("Stop function called, threads signalled to stop, waiting for threads to join");
		try{
			removeFromCacheThread.join();
			writeToSSDThread.join();
			removeFromSSDThread.join();
			writeToHDFSThread.join();
			readFromSSDThread.join();
			readFromHDFSThread.join();
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

//	void readFromSSD(int pageNumber){
//		Page returnPage = SSD.readPage(pageNumber);
//		cache.writePage(returnPage,this);
//		pageIndex.updatePageIndex(pageNumber, 1, -1, -1, -1);
//		readOutputQueue.add(returnPage);
//	}
//
//	void readFromHDFS(int pageNumber){
//		Block returnBlock = HDFSLayer.readBlock(pageNumber, this);
//		Page returnPage = returnBlock.readPage(pageNumber);
//
//		Page[] returnAllPages = returnBlock.getAllPages();
//		for (int i = 0; i < utils.BLOCK_SIZE; i++){
//			// if condition to be added to check the validity
//			int temp = ((returnBlock.blockNumber)<<3)+i;
//			Position p = pageIndex.get(temp);
//			if(p!=null && p.isLocationHDFS() && !p.isDirty() && !p.isLocationCache() && cache.pointersList.get(temp)==null) {
//				cache.writePage(returnAllPages[i],this);
//				pageIndex.updatePageIndex(temp, 1, -1, 1, -1);
//			}
//		}
//		readOutputQueue.add(returnPage);
//	}

	@Nullable void readPage(int pageNumber){
		Page returnPage;
		Position pos = pageIndex.get(pageNumber);

		partiallyStabilize();

		if(pos.isLocationCache()) {
			returnPage =  cache.readPage(pageNumber, false);
			readOutputQueue.add(returnPage);
		}
		else if(pos.isLocationSSD()) {
			readFromSSDQueue.add(pageNumber);
			debugLog("Reading Page " + pageNumber + " from SSD Layer");

//			readFromSSD(pageNumber);
		}
		else if(pos.isLocationHDFS()){
			readFromHDFSQueue.add(pageNumber);
			debugLog("Reading Page " + pageNumber + " from HDFS Layer");

//			readFromHDFS(pageNumber);
		}else {
			System.out.println("Error finding Page: "+pageNumber);
		}
	}

	/**
	 * @param pageNumber is 0 indexed
	 * */
	void writePage(int pageNumber, byte[] pageData){
		Page newPage = new Page(pageNumber, pageData);
		boolean written = cache.writePage(newPage, this);

		if(written){
			pageIndex.updatePageIndex(pageNumber, 1, 0, 0, 1);
		}
		else{
			debugLog("Error in writing Page: "+pageNumber+" to Cache");
		}
	}

	void debugLog(String log){
		if(utils.SHOW_LOG){
			System.out.println(log);
		}
	}

	void partiallyStabilize(){
		while (SSD.writeToSSDQueue.size() > 0){}
		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}

		while (SSD.writeToHDFSQueue.size() > 0){}
		while (SSD.pointersList.size() > utils.MAX_SSD_FULL_SIZE){}

		while (SSD.writeToSSDQueue.size() > 0){}
		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}
	}

	void stabilize(){
		try{Thread.sleep(100);}
		catch(InterruptedException e){}

		while (SSD.writeToSSDQueue.size() > 0){}
		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}

		while (SSD.writeToHDFSQueue.size() > 0){}
		while (SSD.pointersList.size() > utils.MAX_SSD_FULL_SIZE){}
		while (readFromSSDQueue.size() > 0){}
		while (readFromHDFSQueue.size() > 0){}
	}

	void writeToFileBlockServerStatus(){
		System.out.println("Printing BlockServer status:");
		System.out.println("Pages in Cache["+utils.MAX_CACHE_FULL_SIZE+", "+utils.CACHE_SIZE+"]:");
		String s;
		try {
			PrintWriter out = new PrintWriter("cacheContents.txt");
			s = "MAX_CACHE_FULL_SIZE="+utils.MAX_CACHE_FULL_SIZE+" CACHE_SIZE="+utils.CACHE_SIZE+"\n";
			for (int k: cache.pointersList.keySet()){
				s += k+"\n";
			}
			out.print(s);
			out.close();
		}catch(Exception e){e.printStackTrace();}

		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}
		while (SSD.pointersList.size() > utils.MAX_SSD_FULL_SIZE){}
		try {
			PrintWriter out = new PrintWriter("SSDContents.txt");
			s = "MAX_SSD_FULL_SIZE="+utils.MAX_SSD_FULL_SIZE+" SSD_SIZE="+utils.SSD_SIZE+"\n";
			for (int k: SSD.pointersList){
				s += k+"\n";
			}
			out.print(s);
			out.close();
		}catch(Exception e){e.printStackTrace();}


		try {
			PrintWriter out = new PrintWriter("HDFSBufferContents.txt");
			s = "HDFS_BUFFER_SIZE="+utils.HDFS_BUFFER_SIZE*8+"\n";
			for (int k: HDFSLayer.HDFSBufferList.keySet()){
				for (int i=0;i<utils.BLOCK_SIZE;++i){
					int pageNumber = (k<<3) + i;
					Position pos = pageIndex.get(pageNumber);
					if(pos.present && pos.isLocationHDFS()){
						s += pageNumber+"\n";
					}
				}
			}
			out.print(s);
			out.close();
		}catch(Exception e){e.printStackTrace();}


		try {
			PrintWriter out = new PrintWriter("HDFSClusterContents.txt");
			s = "Infinite\n";
			for (int k: HDFSLayer.blockList.keySet()){
				for (int i=0;i<utils.BLOCK_SIZE;++i){
					int pageNumber = (k<<3) + i;
					Position pos = pageIndex.get(pageNumber);
					if(pos.present && pos.isLocationHDFS()){
						s += pageNumber+"\n";
					}
				}
			}
			out.print(s);
			out.close();
		}catch(Exception e){e.printStackTrace();}
	}

	void printBlockServerStatus(){
		while (cache.pointersList.size() > utils.MAX_CACHE_FULL_SIZE){}
		while (SSD.pointersList.size() > utils.MAX_SSD_FULL_SIZE){}

		while (SSD.writeToSSDQueue.size() > 0){}
		while (SSD.writeToHDFSQueue.size() > 0){}

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
		for (int k: HDFSLayer.HDFSBufferList.keySet()){
			for (int i=0;i<utils.BLOCK_SIZE;++i){
				int pageNumber = (k<<3) + i;
				Position pos = pageIndex.get(pageNumber);
				if(pos.present && pos.isLocationHDFS()){
//					System.out.println(pageNumber);
					s += pageNumber+" ";
				}
			}
			s+=" ";
		}
		System.out.println(s);

		s = "";
		System.out.println("Pages in HDFS Cluster:");
		for (int k: HDFSLayer.blockList.keySet()){
			for (int i=0;i<utils.BLOCK_SIZE;++i){
				int pageNumber = (k<<3) + i;
				Position pos = pageIndex.get(pageNumber);
				if(pos.present && pos.isLocationHDFS()){
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