package blockstorage;

import javafx.util.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SSD{

	private String SSD_LOCATION;
	private HDFSLayer HDFSLayer;
	private Utils utils;
	LinkedHashMap<Integer, Boolean> recencyList;
	@NotNull Lock recencyListLock = new ReentrantLock();
	CopyOnWriteArraySet<Integer> pointersList;
	@NotNull Lock pointersListLock = new ReentrantLock();
	@NotNull Lock writeToSSDQueueLock = new ReentrantLock();


	@Nullable
	static Map.Entry<Integer, Boolean> elder = null;

	@NotNull ConcurrentLinkedQueue<Pair<Integer,Integer>> writeToSSDQueue = new ConcurrentLinkedQueue<>();
	@NotNull ConcurrentLinkedQueue<Integer> writeToHDFSQueue = new ConcurrentLinkedQueue<>();

	SSD(HDFSLayer HDFSLayer, Utils utils){
		this.HDFSLayer = HDFSLayer;
		this.utils = utils;
		setSSD_LOCATION();
		this.recencyList = new LinkedHashMap<Integer, Boolean>(utils.SSD_SIZE, 0.75F, false) {
			private static final long serialVersionUID = 1L;
			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, Boolean> eldest) {
				elder =  eldest;
				return size() > utils.SSD_SIZE;
			}
		};
		this.pointersList = new CopyOnWriteArraySet<>();
	}

	void setSSD_LOCATION(){
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}


	void writeSSDPage(int pageNumber, int pointer) {
		writeToSSDQueue.add(new Pair<>(pageNumber, pointer));
	}

	@NotNull Page readSSDPage(int pageNumber){
		File file = new File(SSD_LOCATION + "/" + pageNumber);
		byte[] pageData = new byte[utils.PAGE_SIZE];
		try
		{
			FileInputStream in = new FileInputStream(file);
			in.read(pageData);
			in.close();
		}
		catch(FileNotFoundException e)
		{
			System.out.println("File not found" + e);
		}
		catch(IOException ioe)
		{
			System.out.println("Exception while reading the file " + ioe);
		}

		return new Page(pageNumber, pageData);
	}

	@NotNull Page readPage(int pageNumber) {
		recencyListLock.lock();
		if(recencyList.containsKey(pageNumber)){
			recencyList.remove(pageNumber);
			recencyList.put(pageNumber, true);
		}
		recencyListLock.unlock();
		return readSSDPage(pageNumber);
	}

	void writePage(int pageNumber, int pointer, @NotNull BlockServer server){

		while (pointersList.size() >= utils.SSD_SIZE){
//			System.out.println("waiting for SSD to have space");
			try{Thread.sleep(100);}
			catch(InterruptedException e){}
		}

		if(pointersList.size() >= utils.SSD_SIZE){

			if(pointersList.contains(pageNumber)){
				writeSSDPage(pageNumber, pointer);
				server.debugLog("cache,2,"+pageNumber+", pageNumber " + pageNumber + " added to writeToSSDQueue");
			}else{
				//assume elder is always updated
				/*
				Page temp = readSSDPage(elder.getKey());
				long tempPageNumber= temp.getPageNumber();
				if(server.pageIndex.get(tempPageNumber).isDirty()) {
					HDFSLayer.writePage(temp, server);
					server.updatePageIndex(elder.getKey(), -1, 0, 1, -1);
				}
				recencyList.remove(tempPageNumber);
				*/
				long zero = 0;
//				recencyList.put(zero, true);
//				recencyList.remove(zero);
//				writeToHDFSQueue.add(elder.getKey());
//				WritetoHDFSthread(server);

//				writeSSDPage(pageNumber, pointer);
			}
		}else{
//			if(pointersList.contains(pageNumber)){
//				writeSSDPage(pageNumber, pointer);
//			}else{
//				size.getAndIncrement();
				writeSSDPage(pageNumber, pointer);
				server.debugLog("cache,2,"+pageNumber+", pageNumber " + pageNumber + " added to writeToSSDQueue");
//			}
		}
//		System.out.println("size & max_size = "+size+" "+utils.SSD_SIZE);

	}
}