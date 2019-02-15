package BlockStorage;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class HDFSLayer{

	LinkedHashMap<Long, blockValue> HDFSBufferList;
	block[] HDFSBufferArray;
	int writePointer;
	HashMap<Long,Boolean> blockList ; // all the blocks which HDFS Cluster contains
	Utils utils;

	private static Map.Entry<Long, blockValue> elder = null;

	FileSystemOperations client;
	Configuration config;

	void closeFS(){
		client.closeFS(config);
	}

	HDFSLayer(Utils utils){
		this.utils = utils;
		client = new FileSystemOperations(this.utils);
		config = client.getConfiguration();

		this.HDFSBufferList = new LinkedHashMap<Long, blockValue>(utils.HDFS_BUFFER_SIZE, 0.75F, false) {

			/**
			 * auto generated serialVersionUID
			 */
			private static final long serialVersionUID = 1L;

			protected boolean removeEldestEntry(Map.Entry<Long, blockValue> eldest) {
				elder =  eldest;
				return size() > utils.HDFS_BUFFER_SIZE;
			}
		};
		this.HDFSBufferArray = new block[utils.HDFS_BUFFER_SIZE];
		this.blockList = new HashMap<Long,Boolean>();
		this.writePointer = 0;
	}


	public void writePage(page page, blockServer server){
		HDFSBufferWritePage(page, server);
	}

	public block readBlock(long pageNumber, blockServer server){
		return HDFSBufferReadBlock(pageNumber, server);
	}

	/***
	 * Writes or update the page to the corresponding block
	 * @param page
	 */
	public void HDFSBufferWritePage(page page, blockServer server){
		/***
		 * Update the page if present in the buffer else
		 * get the block that is having this page and update the block
		 */
		try{
			long blockNumber = page.getPageNumber() >> 3;
			if(HDFSBufferList.containsKey(blockNumber)){

				// System.out.println("Hello "+page.getPageNumber());
				int pointer = addWrite(blockNumber, true, server);
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

				block tempBlock = new block(blockNumber,read, utils);
				int pointer = addWrite(blockNumber, true, server);
				//System.out.println(pointer);
				HDFSBufferArray[pointer] = tempBlock;
		}
	}
	catch(IOException e){
		e.printStackTrace();
	}

	}

	public block HDFSBufferReadBlock(long pageNumber, blockServer server){

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
			tempBlock = new block(blockNumber,read, utils);
			int pointer = addWrite(blockNumber, false, server);
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
	 * @param blockDirtyBit
	 * @param server
	 * @return pointer to the HDFSBufferArray
	 */
	public int addWrite(long blockNumber, boolean blockDirtyBit, blockServer server){
		// int answer = 0;
		try{
			if(HDFSBufferList.containsKey(blockNumber)){
				// page already exists in HDFS Buffer
				blockValue val = HDFSBufferList.get(blockNumber);
				int pointer = val.getPointer();
				HDFSBufferList.remove(blockNumber);
				val.setDirtyBit(blockDirtyBit);
				HDFSBufferList.put(blockNumber, val);
				return pointer;
			}else{
				if(writePointer == utils.HDFS_BUFFER_SIZE){
					// victim page removal
					blockValue val = new blockValue();
					HDFSBufferList.put(blockNumber,val);
					if(elder.getValue().getDirtyBit()){
						// write to HDFS cluster
						blockList.put(elder.getKey(),true);
						client.addFile(config, HDFSBufferArray[elder.getValue().getPointer()]);
						long BN = elder.getKey();
						for(int i=0;i<utils.BLOCK_SIZE;++i){
							server.updatePageIndex((BN<<3) + i,-1,-1,1,0);
						}
					}
					int emptyPointer = elder.getValue().getPointer();
					val.setPointer(emptyPointer);
					val.setDirtyBit(blockDirtyBit);
					HDFSBufferList.remove(blockNumber);
					HDFSBufferList.put(blockNumber,val);
					assert (HDFSBufferList.size()<=utils.HDFS_BUFFER_SIZE);

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
		System.out.println("flushing HDFS Buffer");
		for(Long key : HDFSBufferList.keySet()) {
			blockValue x = HDFSBufferList.get(key);
			if(x.getDirtyBit()) {
				try {
					client.addFile(config, HDFSBufferArray[x.getPointer()]);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("flushed HDFS Buffer");

		writePointer=0;
		HDFSBufferList.clear();
	}
}
