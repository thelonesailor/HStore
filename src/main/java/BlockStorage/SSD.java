package BlockStorage;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class SSD{

	private String SSD_LOCATION;
	private Utils utils = new Utils();
	private LinkedHashMap<Long, Boolean> recencyList;
	private HDFSLayer HDFSLayer;
	private static Map.Entry<Long, Boolean> elder = null;
	private int size;
	Queue<page> queue1 = new LinkedList<page>();


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
		// utils utilsObject = new utils();
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}

	void thread1(){
		page page = queue1.remove();
		String fileName = SSD_LOCATION + "/" + page.getPageNumber();

		try {
			FileOutputStream out = new FileOutputStream(fileName);
			out.write(page.getPageData());
			out.close();
		}
		catch (IOException e) {
			System.out.println("Exception Occurred:");
			e.printStackTrace();
		}
	}

	public void writeSSDPage(page page) {
		queue1.add(page);
		thread1();
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

	public void writePage(page page, blockServer server){


		if(size == utils.SSD_SIZE-1){
			if(recencyList.containsKey(page.getPageNumber())){
				writeSSDPage(page);
			}else{
				//assume elder is always updated
				page temp = readSSDPage(elder.getKey());
				long tempPageNumber= temp.getPageNumber();
				if(server.pageIndex.get(tempPageNumber).isDirty()) {
					HDFSLayer.writePage(temp, server);
					server.updatePageIndex(elder.getKey(), -1, -1, 1, -1);
				}
				recencyList.remove(tempPageNumber);
				writeSSDPage(page);
			}
		}else{
			if(recencyList.containsKey(page.getPageNumber())){
				writeSSDPage(page);
			}else{
				size++;
				writeSSDPage(page);
			}
		}
		recencyList.put(page.getPageNumber(), true); //elder is updated
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