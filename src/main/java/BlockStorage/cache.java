package BlockStorage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class cache{

	LinkedHashMap<Long, cacheValue> cacheList;
	Lock cacheListLock = new ReentrantLock();

	byte[][] cacheBuffer;
	AtomicInteger size = new AtomicInteger(0);
	SSD SSD;
	Utils utils;

	ConcurrentLinkedQueue<Integer> EmptyPointers;
	ConcurrentHashMap<Long, Integer> pointersList;
	ConcurrentHashMap<Long, Integer> wasputinpointersList;

	Map.Entry<Long, cacheValue> elder = null;

	cache(SSD SSD, Utils utils){
		this.SSD = SSD;
		this.utils = utils;
		this.cacheList = new LinkedHashMap<Long, cacheValue>(utils.CACHE_SIZE, 0.75F, false) {

			private static final long serialVersionUID = 1L;
			@Override
			protected boolean removeEldestEntry(Map.Entry<Long, cacheValue> eldest) {
				elder =  eldest;
				return size() > utils.CACHE_SIZE;
			}
		};
		this.cacheBuffer = new byte[utils.CACHE_SIZE+1][utils.PAGE_SIZE];

		this.pointersList = new ConcurrentHashMap<>();
		this.wasputinpointersList = new ConcurrentHashMap<>();
		this.EmptyPointers = new ConcurrentLinkedQueue<>();
		for(int i=1;i<=utils.CACHE_SIZE;++i) {
			EmptyPointers.add(i);
		}
	}

	/***
	 * Adding a recently read page to the list
	 * @param pageNumber
	 * @return pointer to the cacheBuffer
	 * Even if page was already in cache remove and add for LRU
	 */
	public int addRead(long pageNumber, boolean forQueue){
//		cacheValue val = cacheList.get(pageNumber);

//		System.out.println("Reading "+pageNumber+" from cache");
		if(!pointersList.containsKey(pageNumber)) {
			System.out.println("Reading "+pageNumber+" from cache");
			assert false;
		}

		int pointer = pointersList.get(pageNumber);
		if(!forQueue){
			cacheListLock.lock();
			cacheList.remove(pageNumber);
			cacheList.put(pageNumber,new cacheValue(pointer));
			cacheListLock.unlock();
		}

		return pointer;
	}
	/***
	 * Adding a recently write page to the list
	 * @param pageNumber
	 * @return pointer to the cacheBuffer
	 */
	public int addWrite(long pageNumber, blockServer server){
		if(pointersList.containsKey(pageNumber)){
			// page already exists in cache
//			cacheValue val = cacheList.get(pageNumber);
			int pointer = pointersList.get(pageNumber);

			cacheListLock.lock();
			cacheList.remove(pageNumber);
			cacheList.put(pageNumber,new cacheValue(pointer));
			cacheListLock.unlock();

			return pointer;
		}else{
			while (size.get() >= utils.CACHE_SIZE){
				System.out.println("waiting for cache to have space");
				try{Thread.sleep(100);}
				catch(InterruptedException e){}
			}
			if(size.get() == utils.CACHE_SIZE){
				// victim page removal
				cacheValue val = new cacheValue();
				cacheList.put(pageNumber,val); // elder gets updated here
				long pageNumberToRemove = elder.getKey();
				int freePointer= elder.getValue().getPointer();
				if(server.pageIndex.get(pageNumberToRemove).isDirty() /*elder.getValue().getDirtyBit()*/){
					SSD.writePage(pageNumberToRemove, freePointer, server);
				}
//				server.updatePageIndex(pageNumberToRemove, 0, 1, -1, -1);
//				cacheList.remove(pageNumberToRemove);


//				cacheList.remove(pageNumber);
//				cacheList.put(pageNumber,new cacheValue(freePointer));

				return elder.getValue().getPointer();
			}else{
				// initial stage
//				cacheValue val = new cacheValue(writePointer);
				int freePointer = EmptyPointers.remove();

				cacheListLock.lock();
				cacheList.remove(pageNumber);
				cacheList.put(pageNumber, new cacheValue(freePointer));
				cacheListLock.unlock();

//	    		System.out.println("added "+pageNumber);

				size.getAndIncrement();
//				if(size.get() == utils.CACHE_SIZE){
//					try{Thread.sleep(1000);}
//					catch(InterruptedException e){}
//				}

				return freePointer;
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
	page readPage(long pageNumber, boolean forQueue){
		int pointer = addRead(pageNumber, forQueue);
		return new page(pageNumber, cacheBuffer[pointer]);
	}

	boolean writePage(page page, blockServer server){
		// System.out.println("CACHE");
		long pageNumber = page.getPageNumber();
		int pointer = addWrite(pageNumber, server);
		cacheBuffer[pointer] = page.getPageData();

//		pointersList.remove(pageNumber);
		pointersList.put(pageNumber, pointer);
		wasputinpointersList.put(pageNumber,pointer);

//		System.out.println("Wrote "+pageNumber+" to cache");
		return true;
	}
}