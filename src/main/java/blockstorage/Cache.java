package blockstorage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Cache {

	LinkedHashMap<Integer, CacheValue> cacheList;
	@NotNull Lock cacheListLock = new ReentrantLock();

	byte[][] cacheBuffer;
//	AtomicInteger size = new AtomicInteger(0);
	SSD SSD;
	Utils utils;

	ConcurrentLinkedQueue<Integer> EmptyPointers;
	ConcurrentHashMap<Integer, Integer> pointersList;
	ConcurrentHashMap<Integer, Integer> wasputinpointersList;

	@Nullable
	static Map.Entry<Integer, CacheValue> elder = null;

	Cache(SSD SSD, Utils utils){
		this.SSD = SSD;
		this.utils = utils;
		this.cacheList = new LinkedHashMap<Integer, CacheValue>(utils.CACHE_SIZE, 0.75F, false) {

			private static final long serialVersionUID = 1L;
			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, CacheValue> eldest) {
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
	 * Adding a recently read Page to the list
	 * @param pageNumber
	 * @return pointer to the cacheBuffer
	 * Even if Page was already in Cache remove and add for LRU
	 */
	public int addRead(int pageNumber, boolean forQueue){
//		CacheValue val = cacheList.get(pageNumber);

//		System.out.println("Reading "+pageNumber+" from Cache");
		if(pointersList.get(pageNumber) == null) {
			System.out.println("Reading "+pageNumber+" from Cache "+wasputinpointersList.contains(pageNumber)+" "+forQueue);
			assert false;
		}

		int pointer = pointersList.get(pageNumber);
		if(!forQueue){
			cacheListLock.lock();
			cacheList.remove(pageNumber);
			cacheList.put(pageNumber,new CacheValue(pointer));
			cacheListLock.unlock();
		}

		return pointer;
	}
	/***
	 * Adding a recently write Page to the list
	 * @param pageNumber
	 * @return pointer to the cacheBuffer
	 */
	synchronized int addWrite(int pageNumber, @NotNull BlockServer server){
		if(pointersList.containsKey(pageNumber)){
			// Page already exists in Cache
//			CacheValue val = cacheList.get(pageNumber);
			int pointer = pointersList.get(pageNumber);

			cacheListLock.lock();
			cacheList.remove(pageNumber);
			cacheList.put(pageNumber,new CacheValue(pointer));
			cacheListLock.unlock();

			return pointer;
		}else{
			while (pointersList.size() >= utils.CACHE_SIZE){
//				System.out.println("waiting for Cache to have space");
				try{Thread.sleep(100);}
				catch(InterruptedException e){}
			}
			if(pointersList.size() == utils.CACHE_SIZE){
				// victim Page removal
				CacheValue val = new CacheValue();
				cacheList.put(pageNumber,val); // elder gets updated here
				int pageNumberToRemove = elder.getKey();
				int freePointer= elder.getValue().getPointer();
				if(server.pageIndex.get(pageNumberToRemove).isDirty() /*elder.getValue().getDirtyBit()*/){
					SSD.writePage(pageNumberToRemove, freePointer, server);
				}
//				server.updatePageIndex(pageNumberToRemove, 0, 1, -1, -1);
//				cacheList.remove(pageNumberToRemove);


//				cacheList.remove(pageNumber);
//				cacheList.put(pageNumber,new CacheValue(freePointer));

				return elder.getValue().getPointer();
			}else{
				// initial stage
//				CacheValue val = new CacheValue(writePointer);
				while (EmptyPointers.isEmpty()){
					try{Thread.sleep(100);}
					catch(InterruptedException e){}
				}
				int freePointer = EmptyPointers.remove();

				cacheListLock.lock();
				cacheList.remove(pageNumber);
				cacheList.put(pageNumber, new CacheValue(freePointer));
				cacheListLock.unlock();

//	    		System.out.println("added "+pageNumber);

//				size.getAndIncrement();

//				if(size.get() == utils.CACHE_SIZE){
//					try{Thread.sleep(1000);}
//					catch(InterruptedException e){}
//				}

				return freePointer;
			}
		}
	}

//	public void resetCache(BlockServer server){
//		for(Long key : cacheList.keySet()) {
//			CacheValue x = cacheList.get(key);
//			SSD.writePage(new Page(key, cacheBuffer[x.getPointer()]), server);
//		}
//
//		writePointer=0;
//		cacheList.clear();
//	}


	/***
	 * It is guaranteed that Page is already in the Cache
	 * @return Page
	 */
	@NotNull Page readPage(int pageNumber, boolean forQueue){
		int pointer = addRead(pageNumber, forQueue);
		return new Page(pageNumber, cacheBuffer[pointer]);
	}

	boolean writePage(@NotNull Page page, @NotNull BlockServer server){
		// System.out.println("CACHE");
		int pageNumber = page.getPageNumber();
		int pointer = addWrite(pageNumber, server);
		cacheBuffer[pointer] = page.getPageData();

		pointersList.remove(pageNumber);
		pointersList.put(pageNumber, pointer);
		wasputinpointersList.put(pageNumber,pointer);

//		System.out.println("Wrote "+pageNumber+" to Cache");
		return true;
	}
}