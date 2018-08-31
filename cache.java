package BlockStorage;

import java.util.LinkedHashMap;
import java.util.Map;

public class cache{

    LinkedHashMap<Long, cacheValue> cacheList;
    byte[][] cacheBuffer;
    int writePointer;
    SSD SSD;
    Utils utils = new Utils();

    private static Map.Entry<Long, cacheValue> elder = null;

    public cache(SSD SSD){
    		this.SSD = SSD;
        this.cacheList = new LinkedHashMap<Long, cacheValue>(utils.CACHE_SIZE, 0.75F, false) {

            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			protected boolean removeEldestEntry(Map.Entry<Long, cacheValue> eldest) {
                elder =  eldest;
                return size() > utils.CACHE_SIZE;
            }
        };
        this.cacheBuffer = new byte[utils.CACHE_SIZE][utils.PAGE_SIZE];
        this.writePointer = 0;
    }

    /***
     * Adding a recently read page to the list
     * @param pageNumber
     * @return pointer to the cacheBuffer
     */
    public int addRead(long pageNumber){
        cacheValue val = cacheList.get(pageNumber);
        int pointer = val.getPointer();
        cacheList.remove(pageNumber);
        cacheList.put(pageNumber,val);
        return pointer;
    }
    /***
     * Adding a recently write page to the list
     * @param pageNumber
     * @param dirtyBit
     * @return pointer to the cacheBuffer
     */
    public int addWrite(long pageNumber, boolean dirtyBit){
        if(cacheList.containsKey(pageNumber)){
            // page already exists in cache
            cacheValue val = cacheList.get(pageNumber);
            int pointer = val.getPointer();
            cacheList.remove(pageNumber);
            val.setDirtyBit(dirtyBit);
            cacheList.put(pageNumber,val);
            return pointer;
        }else{
            if(writePointer == utils.CACHE_SIZE){
                // victim page removal
                cacheValue val = new cacheValue();
                cacheList.put(pageNumber,val);
                if(elder.getValue().getDirtyBit()){
                    SSD.writePage(new page((long) elder.getKey(), cacheBuffer[elder.getValue().getPointer()]));
                }
                val.setPointer(elder.getValue().getPointer());
                val.setDirtyBit(dirtyBit);
                return elder.getValue().getPointer();
            }else{
                // initial stage
                cacheValue val = new cacheValue(writePointer,dirtyBit);
                cacheList.put(pageNumber,val);
                writePointer++;
                return (writePointer - 1);
            }
        }
    }

    /***
     * removes the tail entry and returns pointer
     * @return pointer
     */
    public int getBlankPage(){
        return 0;
    }

    /***
     * Flushes the dirty pages into SSD.
     * @param numberOfPages
     * @return false if the number of dirty pages < numberOfPages.
     */
    public boolean cleanWriteChunk(int numberOfPages){
        return true;
    }

    /***
     *
     * @return
     */
    public page readPage(long pageNumber){
        int pointer = addRead(pageNumber);
        return new page(pageNumber, cacheBuffer[pointer]);
    }

    public void writePage(page page, boolean dirtyBit){
        // System.out.println("CACHE");
        int pointer = addWrite(page.getPageNumber(), dirtyBit);
        cacheBuffer[pointer] = page.getPageData();
    }
}