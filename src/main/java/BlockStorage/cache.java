package BlockStorage;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class cache{

	LinkedHashMap<Long, cacheValue> cacheList;
	byte[][] cacheBuffer;
	int writePointer;
	SSD SSD;
	Utils utils = new Utils();

	private static Map.Entry<Long, cacheValue> elder = null;

	cache(SSD SSD){
		this.SSD = SSD;
		this.cacheList = new LinkedHashMap<Long, cacheValue>(utils.CACHE_SIZE, 0.75F, false) {

			private static final long serialVersionUID = 1L;

			protected boolean removeEldestEntry(Map.Entry<Long, cacheValue> eldest) {
				elder =  eldest;
				return size() > utils.CACHE_SIZE;
			}
		};
		this.cacheBuffer = new byte[utils.CACHE_SIZE][utils.PAGE_SIZE];
		this.writePointer = 1;
	}

	/***
	 * Adding a recently read page to the list
	 * @param pageNumber
	 * @return pointer to the cacheBuffer
	 * Even if page was already in cache remove and add for LRU
	 */
	public int addRead(long pageNumber){
		cacheValue val = cacheList.get(pageNumber);
//		 System.out.println(pageNumber);
		int pointer = val.getPointer();
		cacheList.remove(pageNumber);
		cacheList.put(pageNumber,new cacheValue(pointer));
		return pointer;
	}
	/***
	 * Adding a recently write page to the list
	 * @param pageNumber
	 * @return pointer to the cacheBuffer
	 */
	public int addWrite(long pageNumber, blockServer server){
		if(cacheList.containsKey(pageNumber)){
			// page already exists in cache
			cacheValue val = cacheList.get(pageNumber);
			int pointer = val.getPointer();
			cacheList.remove(pageNumber);
			cacheList.put(pageNumber,new cacheValue(pointer));
			return pointer;
		}else{
			if(writePointer == utils.CACHE_SIZE){
				// victim page removal
				cacheValue val = new cacheValue();
				cacheList.put(pageNumber,val); // elder gets updated here
				if(server.pageIndex.get(elder.getKey()).isDirty() /*elder.getValue().getDirtyBit()*/){
					SSD.writePage(new page(elder.getKey(), cacheBuffer[elder.getValue().getPointer()]), server);
				}
				server.updatePageIndex(elder.getKey(), 0, 1, -1, -1);
				cacheList.remove(elder.getKey());


				cacheList.remove(pageNumber);
				cacheList.put(pageNumber,new cacheValue(elder.getValue().getPointer()));

				return elder.getValue().getPointer();
			}else{
				// initial stage
//				cacheValue val = new cacheValue(writePointer);
				cacheList.put(pageNumber, new cacheValue(writePointer));
//				 System.out.println("added "+pageNumber);
				writePointer++;
				return (writePointer - 1);
			}
		}
	}

//	public void resetCache(blockServer server){
//		for(Long key : cacheList.keySet()) {
//			cacheValue x = cacheList.get(key);
//			SSD.writePage(new page(key, cacheBuffer[x.getPointer()]), server);
//		}
//
//		writePointer=0;
//		cacheList.clear();
//	}


	/***
	 * It is guaranteed that page is already in the cache
	 * @return page
	 */
	page readPage(long pageNumber){
		int pointer = addRead(pageNumber);
		return new page(pageNumber, cacheBuffer[pointer]);
	}

	void writePage(page page, blockServer server){
		// System.out.println("CACHE");
		int pointer = addWrite(page.getPageNumber(), server);
		cacheBuffer[pointer] = page.getPageData();
	}
}