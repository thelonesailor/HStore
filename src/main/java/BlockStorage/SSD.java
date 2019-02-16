package BlockStorage;

import javafx.util.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SSD{

	private String SSD_LOCATION;
	private HDFSLayer HDFSLayer;
	private Utils utils;
	LinkedHashMap<Long, Boolean> recencyList;
	Lock recencyListLock = new ReentrantLock();
	List<Long> pointersList;


	static Map.Entry<Long, Boolean> elder = null;
	AtomicInteger size = new AtomicInteger(0);

	ConcurrentLinkedQueue<Pair<Long,Integer>> WritetoSSDqueue = new ConcurrentLinkedQueue<>();
	ConcurrentLinkedQueue<Long> WritetoHDFSqueue = new ConcurrentLinkedQueue<>();

	SSD(HDFSLayer HDFSLayer, Utils utils){
		this.HDFSLayer = HDFSLayer;
		this.utils = utils;
		setSSD_LOCATION();
		this.recencyList = new LinkedHashMap<Long, Boolean>(utils.SSD_SIZE, 0.75F, false) {
			private static final long serialVersionUID = 1L;
			@Override
			protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
				elder =  eldest;
				return size() > utils.SSD_SIZE;
			}
		};
		this.pointersList = new CopyOnWriteArrayList<>();
	}

	void setSSD_LOCATION(){
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}


	void writeSSDPage(long pageNumber, int pointer) {
		WritetoSSDqueue.add(new Pair<>(pageNumber, pointer));
		if(utils.SHOW_LOG)
			System.out.println("page " + pageNumber + " added to WritetoSSDqueue");
	}

	page readSSDPage(long pageNumber){
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

		return new page(pageNumber, pageData);
	}

	page readPage(long pageNumber) {
		recencyListLock.lock();
		recencyList.remove(pageNumber);
		recencyList.put(pageNumber, true);
		recencyListLock.unlock();
		return readSSDPage(pageNumber);
	}


//	void WritetoHDFSthread(blockServer server){
//		long pageNumber = WritetoHDFSqueue.remove();
//		File file = new File(SSD_LOCATION + "/" + pageNumber);
//		byte[] pageData = new byte[utils.PAGE_SIZE];
//		try
//		{
//			FileInputStream in = new FileInputStream(file);
//			in.read(pageData);
//			in.close();
//		}
//		catch(FileNotFoundException e)
//		{
//			System.out.println("File not found" + e);
//		}
//		catch(IOException ioe)
//		{
//			System.out.println("Exception while reading the file " + ioe);
//		}
//		page page = new page(pageNumber, pageData);
//
//		if(server.pageIndex.get(pageNumber).isDirty()) {
//			HDFSLayer.writePage(page, server);
//			server.updatePageIndex(pageNumber, -1, 0, 1, -1);
//		}
//		else {
//			server.updatePageIndex(pageNumber, -1, 0, -1, -1);
//		}
//
//		recencyList.remove(pageNumber);
//		size.getAndDecrement();
////		System.out.println(pageNumber+" removed from SSD.");
//		file.delete(); //remove the file from actual SSD
//	}

	void writePage(long pageNumber,int pointer, blockServer server){

		while (size.get() >= utils.SSD_SIZE){
//			System.out.println("waiting for SSD to have space");
			try{Thread.sleep(100);}
			catch(InterruptedException e){}
		}

		if(size.get() >= utils.SSD_SIZE){

			if(pointersList.contains(pageNumber)){
				writeSSDPage(pageNumber, pointer);
			}else{
				//assume elder is always updated
				/*
				page temp = readSSDPage(elder.getKey());
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
//				WritetoHDFSqueue.add(elder.getKey());
//				WritetoHDFSthread(server);

//				writeSSDPage(pageNumber, pointer);
			}
		}else{
//			if(pointersList.contains(pageNumber)){
//				writeSSDPage(pageNumber, pointer);
//			}else{
//				size.getAndIncrement();
				writeSSDPage(pageNumber, pointer);
//			}
		}
//		System.out.println("size & max_size = "+size+" "+utils.SSD_SIZE);

	}

//	public void resetSSD(blockServer server){
//		for(Long key : recencyList.keySet()) {
//			page temp = readSSDPage(key);
//			HDFSLayer.writePage(temp, server);
//		}
//
//		size=0;
//		recencyList.clear();
//	}

}