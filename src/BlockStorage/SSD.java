package BlockStorage;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class SSD{

	page[] SSDBuffer, SSDToHDFSBuffer;
	int SSDBufferPointer, SSDToHDFSBufferPointer;
	String SSD_LOCATION;
	Utils utils = new Utils();
	LinkedHashMap<Long, Boolean> recencyList;
	HDFSLayer HDFSLayer;
	private static Map.Entry<Long, Boolean> elder = null;
	int size;

	public SSD(HDFSLayer HDFSLayer){
		size = 0;
			this.HDFSLayer = HDFSLayer;
		this.SSDBuffer = new page[utils.CHUNK_SIZE];
		setSSD_LOCATION();
		this.recencyList = new LinkedHashMap<Long, Boolean>(utils.SSD_SIZE, 0.75F, false) {

			/**
			 * 
			 */
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

	public void flushSSDBufferToSSD(){
		for (int i = 0; i < utils.CHUNK_SIZE; i++) {
			String fileName = SSD_LOCATION + "/" + SSDBuffer[i].getPageNumber();
			try
			{
				FileOutputStream out = new FileOutputStream(fileName);
				out.write(SSDBuffer[i].getPageData());
				out.close();
			}
			catch (IOException e)
			{
				System.out.println("Exception Occurred:");
				e.printStackTrace();
			}
		}
		SSDBufferPointer = 0;
	}

	public void writeSSDPage(page page) {
		String fileName = SSD_LOCATION + "/" + page.getPageNumber();
		try
		{
			FileOutputStream out = new FileOutputStream(fileName);
			out.write(page.getPageData());
			out.close();
		}
		catch (IOException e)
		{
			System.out.println("Exception Occurred:");
			e.printStackTrace();
		}
	}

	public page readSSDPage(long pageNumber){
		File file = new File(SSD_LOCATION + "/" + pageNumber);
		byte[] pageData = new byte[utils.PAGE_SIZE];
		try
		{
			FileInputStream fin = new FileInputStream(file);
			fin.read(pageData);
			fin.close();
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

	public void writePage(page page){
//        SSDBuffer[this.SSDBufferPointer] = page;
//        SSDBufferPointer++;
//        if(SSDBufferPointer == utils.CHUNK_SIZE){
//            flushSSDBufferToSSD();
//        }

		if(size == utils.SSD_SIZE-1){
			if(recencyList.containsKey(page.getPageNumber())){
				writeSSDPage(page);
			}else{
				page temp = readSSDPage(elder.getKey());
				HDFSLayer.writePage(temp);
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

	public void flushSSDToHDFS(){
		// TODO : create the recency list

	}
}