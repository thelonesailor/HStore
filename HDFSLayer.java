package BlockStorage;

import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        this.HDFSBufferList = new LinkedHashMap<Long, blockValue>(utils.CACHE_SIZE, 0.75F, false) {

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

    public void createImage(){

        /***
         * TODO : create folder in HDFS
         */

    }

    public void writePage(page page){
           HDFSBufferWrite(page, true);
        
    }

    public block readBlock(long pageNumber){
        return HDFSBufferReadBlock(pageNumber);
    }

    /***
     * Writes or update the page to the corresponding block
     * @param page
     */
    public void HDFSBufferWrite(page page, boolean dirtyBit){
        /***
         * Update the page if present in the buffer else
         * get the block that is having this page and update the block
         */
        try{
            long blockNumber = page.getPageNumber() / 8;
        if(HDFSBufferList.containsKey(blockNumber)){

            // System.out.println("Hello "+page.getPageNumber());
            int pointer = addWrite(blockNumber, dirtyBit);
            //System.out.println(pointer);
            HDFSBufferArray[pointer].addPageToBlock(page);
        }else{
            /***
             * TODO : get the block from HDFS
             */
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
        
        long blockNumber = pageNumber / 8;
        block tempBlock = null;
try{
        if(HDFSBufferList.containsKey(blockNumber)){
            int pointer = addRead(blockNumber);
            tempBlock =  HDFSBufferArray[pointer];
        }else{
            /***
             * TODO : get the block from HDFS
             */
        	byte[] read;
            if(blockList.containsKey(blockNumber)){
                read = client.readFile(config, blockNumber);
            }else{
                read = new byte[8*utils.PAGE_SIZE];
            }	
            tempBlock = new block(blockNumber,read);
            int pointer = addWrite(blockNumber, false);
            HDFSBufferArray[pointer] = tempBlock;
        }}catch(IOException e){
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
     * @param pageNumber
     * @param dirtyBit
     * @return pointer to the HDFSBufferArray
     */
    public int addWrite(long blockNumber, boolean dirtyBit){
        // int answer = 0;
        try{
        if(HDFSBufferList.containsKey(blockNumber)){
            // page already exists in cache
            blockValue val = HDFSBufferList.get(blockNumber);
            int pointer = val.getPointer();
            HDFSBufferList.remove(blockNumber);
            val.setDirtyBit(dirtyBit);
            HDFSBufferList.put(blockNumber, val);
            return pointer;
        }else{
            if(writePointer == utils.CACHE_SIZE){
                // victim page removal
                blockValue val = new blockValue();
                HDFSBufferList.put(blockNumber,val);
                if(elder.getValue().getDirtyBit()){
                    // TODO : write to HDFS
                        blockList.put(elder.getKey(),true);
                		client.addFile(config, HDFSBufferArray[elder.getValue().getPointer()]);
                }
                val.setPointer(elder.getValue().getPointer());
                val.setDirtyBit(dirtyBit);
                return elder.getValue().getPointer();
            }else{
                // initial stage
                blockValue val = new blockValue(writePointer,dirtyBit);
                HDFSBufferList.put(blockNumber, val);
                writePointer++;
                return (writePointer - 1);
            }
        }
    }catch(IOException e){
  e.printStackTrace();
}   return 0;
}
}
