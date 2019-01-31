package BlockStorage;


import javafx.util.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SSD{

	private String SSD_LOCATION;
	private Utils utils = new Utils();
	LinkedHashMap<Long, Boolean> recencyList;
	private HDFSLayer HDFSLayer;
	private static Map.Entry<Long, Boolean> elder = null;
	int size;

//	Queue<Pair<Long,Integer>> WritetoSSDqueue = new LinkedList<>();
//	Queue<Long> RemovalFromSSDqueue = new LinkedList<>();

	ConcurrentLinkedQueue<Pair<Long,Integer>> WritetoSSDqueue = new ConcurrentLinkedQueue<>();
	ConcurrentLinkedQueue<Long> RemovalFromSSDqueue = new ConcurrentLinkedQueue<>();


	public SSD(HDFSLayer HDFSLayer){
		this.size = 0;
		this.HDFSLayer = HDFSLayer;
		setSSD_LOCATION();
		this.recencyList = new LinkedHashMap<Long, Boolean>(utils.SSD_SIZE, 0.75F, false) {
			private static final long serialVersionUID = 1L;
			@Override
			protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
				elder =  eldest;
				return size() > utils.SSD_SIZE;
			}
		};
	}

	public void setSSD_LOCATION(){
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}

//	void WritetoSSDthread(){
//		page page = WritetoSSDqueue.remove();
//		String fileName = SSD_LOCATION + "/" + page.getPageNumber();
//
//		try {
//			FileOutputStream out = new FileOutputStream(fileName);
//			out.write(page.getPageData());
//			out.close();
//		}
//		catch (IOException e) {
//			System.out.println("Exception Occurred:");
//			e.printStackTrace();
//		}
//	}

	public void writeSSDPage(long pageNumber, int pointer) {
		WritetoSSDqueue.add(new Pair<>(pageNumber, pointer));
//		WritetoSSDthread();
	}

	public page readSSDPage(long pageNumber){
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

	public page readPage(long pageNumber) {
			recencyList.put(pageNumber, true);
			return readSSDPage(pageNumber);
	}


	void RemovalFromSSDthread(blockServer server){
		long pageNumber = RemovalFromSSDqueue.remove();
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
		page page = new page(pageNumber, pageData);

		if(server.pageIndex.get(pageNumber).isDirty()) {
			HDFSLayer.writePage(page, server);
			server.updatePageIndex(pageNumber, -1, 0, 1, -1);
		}
		else {
			server.updatePageIndex(pageNumber, -1, 0, -1, -1);
		}

		recencyList.remove(pageNumber);
		--size;
//		System.out.println(pageNumber+" removed from SSD.");
		file.delete(); //remove the file from actual SSD
	}

	public void writePage(long pageNumber,int pointer, blockServer server){


		if(size >= utils.SSD_SIZE-1){

			if(recencyList.containsKey(pageNumber)){
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
				recencyList.put(zero, true);
				recencyList.remove(zero);
				RemovalFromSSDqueue.add(elder.getKey());
				RemovalFromSSDthread(server);

				writeSSDPage(pageNumber, pointer);
			}
		}else{
			if(recencyList.containsKey(pageNumber)){
				writeSSDPage(pageNumber, pointer);
			}else{
				size++;
				writeSSDPage(pageNumber, pointer);
			}
		}
//		System.out.println("size & max_size = "+size+" "+utils.SSD_SIZE);
		recencyList.put(pageNumber, true); //elder is updated
	}

	public void resetSSD(blockServer server){
		for(Long key : recencyList.keySet()) {
			page temp = readSSDPage(key);
			HDFSLayer.writePage(temp, server);
		}

		size=0;
		recencyList.clear();
	}

}