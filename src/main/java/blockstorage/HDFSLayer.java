package blockstorage;

import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HDFSLayer{

	LinkedHashMap<Integer, BlockValue> HDFSBufferList;
	@NotNull Lock HDFSBufferListLock = new ReentrantLock();
	LinkedHashMap<Integer, BlockValue> wasAddedToHDFSBufferList;
	Block[] HDFSBufferArray;
	int writePointer;
	ConcurrentHashMap<Integer,Boolean> blockList ; // all the blocks which HDFS Cluster contains
	ConcurrentHashMap<Integer,Boolean> wasAddedToblockList ; // all the blocks which HDFS Cluster contains

	Utils utils;

	@Nullable
	private static Map.Entry<Integer, BlockValue> elder = null;

	FileSystemOperations client;
	Configuration config;

	void closeFS(){
		client.closeFS(config);
	}

	HDFSLayer(Utils utils){
		this.utils = utils;
		client = new FileSystemOperations(this.utils);
		config = client.getConfiguration();

		this.wasAddedToHDFSBufferList = new LinkedHashMap<>();
		this.HDFSBufferList = new LinkedHashMap<Integer, BlockValue>(utils.HDFS_BUFFER_SIZE, 0.75F, false) {

			/**
			 * auto generated serialVersionUID
			 */
			private static final long serialVersionUID = 1L;

			protected boolean removeEldestEntry(Map.Entry<Integer, BlockValue> eldest) {
				elder =  eldest;
				return size() > utils.HDFS_BUFFER_SIZE;
			}
		};
		this.HDFSBufferArray = new Block[utils.HDFS_BUFFER_SIZE];
		this.blockList = new ConcurrentHashMap<>();
		this.wasAddedToblockList = new ConcurrentHashMap<>();
		this.writePointer = 0;
	}


	void writePage(@NotNull Page page, @NotNull BlockServer server){
		HDFSBufferWritePage(page, server);
	}

	@Nullable Block readBlock(int pageNumber, @NotNull BlockServer server){
		return HDFSBufferReadBlock(pageNumber, server);
	}

	/***
	 * Writes or update the Page to the corresponding Block
	 * @param page
	 */
	synchronized void HDFSBufferWritePage(@NotNull Page page, @NotNull BlockServer server){
		/***
		 * Update the Page if present in the buffer else
		 * get the Block that is having this Page and update the Block
		 */
		try{
			int blockNumber = page.getPageNumber() >> 3;
			if(HDFSBufferList.containsKey(blockNumber)){

				// System.out.println("Hello "+Page.getPageNumber());
				int pointer = addWrite(blockNumber, true, server);
				//System.out.println(pointer);
				HDFSBufferArray[pointer].addPageToBlock(page);
			}
			else{
				// System.out.println("Hello "+Page.getPageNumber());
				byte[] read;
				if(blockList.containsKey(blockNumber)){
					read = client.readFile(config, blockNumber);
				}else{
					read = new byte[8*utils.PAGE_SIZE];
				}

				System.arraycopy(page.getPageData(), 0, read, page.getPageNumber()%8 *utils.PAGE_SIZE, utils.PAGE_SIZE);

				Block tempBlock = new Block(blockNumber,read, utils);
				int pointer = addWrite(blockNumber, true, server);
				//System.out.println(pointer);
				HDFSBufferArray[pointer] = tempBlock;
			}
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}

	@Nullable Block HDFSBufferReadBlock(int pageNumber, @NotNull BlockServer server){

		int blockNumber = pageNumber >> 3;
		Block tempBlock = null;
	try{
		if(HDFSBufferList.containsKey(blockNumber)){
			HDFSBufferListLock.lock();
			int pointer = addRead(blockNumber);
			HDFSBufferListLock.unlock();
			tempBlock =  HDFSBufferArray[pointer];
		}else{
			// get the Block from HDFS cluster
			byte[] read;
//			try{
//				Thread.sleep(30);
//			}
//			catch(InterruptedException e){
//				System.out.println("InterruptedException in HDFSBufferReadBlock: " + e);
//			}

			if(blockList.containsKey(blockNumber)){
				read = client.readFile(config, blockNumber);
			}else{
				System.out.println(blockList.containsKey(blockNumber));
				int lmao=34;
//				assert false;
				read = new byte[8*utils.PAGE_SIZE];
			}
			tempBlock = new Block(blockNumber,read, utils);
//			int pointer = addWrite(blockNumber, false, server);
			int pointer = addWrite(blockNumber, false, server);
			HDFSBufferArray[pointer] = tempBlock;
		}
	}catch(IOException e){
	  e.printStackTrace();
	}

		return tempBlock;
	}

	int addRead(int blockNumber){
		BlockValue val = HDFSBufferList.get(blockNumber);
		int pointer = val.getPointer();
		HDFSBufferList.remove(blockNumber);
		HDFSBufferList.put(blockNumber, val);
		wasAddedToHDFSBufferList.put(blockNumber, val);
		return pointer;
	}
	/***
	 * Adding a recently write Page to the list
	 * @param blockNumber (pageNumber)
	 * @param blockDirtyBit
	 * @param server
	 * @return pointer to the HDFSBufferArray
	 */
	synchronized int addWrite(int blockNumber, boolean blockDirtyBit, @NotNull BlockServer server){
		// int answer = 0;
		try{
			HDFSBufferListLock.lock();
			if(HDFSBufferList.containsKey(blockNumber)){
				// Page already exists in HDFS Buffer
				BlockValue val = HDFSBufferList.get(blockNumber);
				int pointer = val.getPointer();
				val.setDirtyBit(val.getDirtyBit() || blockDirtyBit);
				HDFSBufferList.remove(blockNumber);
				HDFSBufferList.put(blockNumber, val);
				HDFSBufferListLock.unlock();
				return pointer;
			}else{
				if(writePointer == utils.HDFS_BUFFER_SIZE){
					// victim Page removal
					BlockValue val = new BlockValue();
//					HDFSBufferList.put(blockNumber,val); //elder updated
					//TODO: problem is that blockToRemove is removed from HDFSBufferList
					int blockToRemove = elder.getKey();
					if(elder.getValue().getDirtyBit()){
						// write to HDFS cluster

						client.addFile(config, HDFSBufferArray[elder.getValue().getPointer()]);
//						System.out.println(blockToRemove+" added to blockList");
						for(int i=0;i<utils.BLOCK_SIZE;++i){
							int pageNumber = (blockToRemove<<3) + i;
							if(server.pageIndex.get(pageNumber) != null){
								server.pageIndex.updatePageIndex((blockToRemove<<3) + i,-1,-1,1,0);
							}
						}
						//TODO: problem is that blockToRemove is added now to BlockList
						blockList.put(blockToRemove,true);
						wasAddedToblockList.put(blockToRemove, true);

					}
					HDFSBufferList.remove(blockToRemove);

					BlockValue v2 = elder.getValue();
					int emptyPointer = v2.getPointer();
					val.setPointer(emptyPointer);
					val.setDirtyBit(v2.getDirtyBit() || blockDirtyBit);

					HDFSBufferList.remove(blockNumber);
					HDFSBufferList.put(blockNumber,val);
					wasAddedToHDFSBufferList.put(blockNumber, val);

					if(HDFSBufferList.size() > utils.HDFS_BUFFER_SIZE){
						System.out.println("ERROR in HDFSBufferList "+HDFSBufferList.size()+" > "+utils.HDFS_BUFFER_SIZE);
						assert false;
					}

					HDFSBufferListLock.unlock();
					return emptyPointer;
				}else if(writePointer < utils.HDFS_BUFFER_SIZE){
					// initial stage
					BlockValue val = new BlockValue(writePointer,true);
					assert HDFSBufferList.size() == writePointer;
					HDFSBufferList.put(blockNumber, val);
					wasAddedToHDFSBufferList.put(blockNumber, val);
					writePointer++;

					HDFSBufferListLock.unlock();
					return (writePointer - 1);
				}
				else{
					HDFSBufferListLock.unlock();
					assert false;
				}
			}

		}catch(IOException e){
			e.printStackTrace();
		}
		return 0;
	}

	void flushHDFSBuffer(){
		System.out.println("flushing HDFS Buffer");
		for(Integer key : HDFSBufferList.keySet()) {
			BlockValue x = HDFSBufferList.get(key);
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
