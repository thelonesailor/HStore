package BlockStorage;

import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class HDFSLayer{

	LinkedHashMap<Long, blockValue> HDFSBufferList;
	block[] HDFSBufferArray;
	int writePointer;
	HashMap<Long,Boolean> blockList ;
	Utils utils = new Utils();

	private static Map.Entry<Long, blockValue> elder = null;

	FileSystemOperations client = new FileSystemOperations();

	Configuration config = client.getConfiguration();

	public HDFSLayer(){
		this.HDFSBufferList = new LinkedHashMap<Long, blockValue>(utils.BUFFER_SIZE, 0.75F, false) {

			/**
			 * auto generated serialVersionUID
			 */
			private static final long serialVersionUID = 1L;

			protected boolean removeEldestEntry(Map.Entry<Long, blockValue> eldest) {
				elder =  eldest;
				return size() > utils.CACHE_SIZE;
			}
		};
		this.HDFSBufferArray = new block[utils.CACHE_SIZE];
		this.blockList = new HashMap<Long,Boolean>();
		this.writePointer = 0;
	}


	public void writePage(page page, blockServer server){
		HDFSBufferWritePage(page, true);
	}

	public block readBlock(long pageNumber){
		return HDFSBufferReadBlock(pageNumber);
	}

	/***
	 * Writes or update the page to the corresponding block
	 * @param page
	 */
	public void HDFSBufferWritePage(page page, boolean dirtyBit){
		/***
		 * Update the page if present in the buffer else
		 * get the block that is having this page and update the block
		 */
		assert dirtyBit;
		try{
			long blockNumber = page.getPageNumber() >> 3;
			if(HDFSBufferList.containsKey(blockNumber)){

				// System.out.println("Hello "+page.getPageNumber());
				int pointer = addWrite(blockNumber, dirtyBit);
				//System.out.println(pointer);
				HDFSBufferArray[pointer].addPageToBlock(page);
			}
			else{
				// System.out.println("Hello "+page.getPageNumber());
				byte[] read;
				if(blockList.containsKey(blockNumber)){
					read = client.readFile(config, blockNumber);
				}else{
					read = new byte[8*utils.PAGE_SIZE];
				}

				System.arraycopy(page.getPageData(), 0, read, ((int)(page.getPageNumber()%8))*utils.PAGE_SIZE, utils.PAGE_SIZE);

				block tempBlock = new block(blockNumber,read);
				int pointer = addWrite(blockNumber, dirtyBit);
				//System.out.println(pointer);
				HDFSBufferArray[pointer] = tempBlock;
		}
	}
	catch(IOException e){
		e.printStackTrace();
	}

	}

	public block HDFSBufferReadBlock(long pageNumber){

		long blockNumber = pageNumber >> 3;
		block tempBlock = null;
	try{
		if(HDFSBufferList.containsKey(blockNumber)){
			int pointer = addRead(blockNumber);
			tempBlock =  HDFSBufferArray[pointer];
		}else{
			// get the block from HDFS cluster
			byte[] read;
			if(blockList.containsKey(blockNumber)){
				read = client.readFile(config, blockNumber);
			}else{
				read = new byte[8*utils.PAGE_SIZE];
			}
			tempBlock = new block(blockNumber,read);
			int pointer = addWrite(blockNumber, false);
			HDFSBufferArray[pointer] = tempBlock;
		}
	}catch(IOException e){
	  e.printStackTrace();
	}

		return tempBlock;
	}

	public int addRead(long blockNumber){
		blockValue val = HDFSBufferList.get(blockNumber);
		int pointer = val.getPointer();
		HDFSBufferList.remove(blockNumber);
		HDFSBufferList.put(blockNumber, val);
		return pointer;
	}
	/***
	 * Adding a recently write page to the list
	 * @param blockNumber (pageNumber)
	 * @param dirtyBit
	 * @return pointer to the HDFSBufferArray
	 */
	public int addWrite(long blockNumber, boolean dirtyBit){
		// int answer = 0;
		try{
			if(HDFSBufferList.containsKey(blockNumber)){
				// page already exists in HDFS Buffer
				blockValue val = HDFSBufferList.get(blockNumber);
				int pointer = val.getPointer();
				HDFSBufferList.remove(blockNumber);
				val.setDirtyBit(dirtyBit);
				HDFSBufferList.put(blockNumber, val);
				return pointer;
			}else{
				if(writePointer == utils.BUFFER_SIZE){
					// victim page removal
					blockValue val = new blockValue();
					HDFSBufferList.put(blockNumber,val);
					if(elder.getValue().getDirtyBit()){
						// write to HDFS cluster
						blockList.put(elder.getKey(),true);
						client.addFile(config, HDFSBufferArray[elder.getValue().getPointer()]);
					}
					int emptyPointer = elder.getValue().getPointer();
					val.setPointer(emptyPointer);
					val.setDirtyBit(dirtyBit);
					HDFSBufferList.remove(blockNumber);
					HDFSBufferList.put(blockNumber,val);
					assert (HDFSBufferList.size()<=utils.BUFFER_SIZE);

					return emptyPointer;
				}else{
					// initial stage
					blockValue val = new blockValue(writePointer,true);
					HDFSBufferList.put(blockNumber, val);
					writePointer++;
					return (writePointer - 1);
				}
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		return 0;
	}

	public void flushHDFSbuffer(){
		for(Long key : HDFSBufferList.keySet()) {
			blockValue x = HDFSBufferList.get(key);
			try{
				client.addFile(config, HDFSBufferArray[x.getPointer()]);
			}catch(IOException e){
				e.printStackTrace();
			}
		}

		writePointer=0;
		HDFSBufferList.clear();
	}
}
