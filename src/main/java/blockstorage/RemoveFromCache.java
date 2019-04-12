package blockstorage;

import java.util.Map;

public class RemoveFromCache implements Runnable{
	private Cache cache;
	SSD SSD;
	BlockServer server;
	private Utils utils;
	private int m1 = -1;

	RemoveFromCache(Cache cache, SSD SSD, BlockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.utils = utils;
	}

	private void doWork() throws InterruptedException{
		if((cache.cacheList.size() > utils.MAX_CACHE_FULL_SIZE )||( server.removeFromCacheStop && (cache.cacheList.size() > 0))) {
			CacheValue val = new CacheValue();

			cache.cacheListLock.lock();
			cache.cacheList.put(m1,val); // elder gets updated here
			cache.cacheList.remove(m1);
			cache.cacheList.remove(m1);
//			Cache.cacheListLock.unlock();

			Map.Entry<Integer, CacheValue>  eld = Cache.elder;
			int pageNumberToRemove = eld.getKey();
			int pointer = eld.getValue().getPointer();

//			Cache.cacheListLock.lock();
			cache.cacheList.remove(pageNumberToRemove);
			cache.cacheListLock.unlock();

			SSD.writePage(pageNumberToRemove, pointer, server);

		}
		else{
			Thread.sleep(10);
		}
	}
	public void run(){

		while (true) {
//			System.out.println("remove from Cache!!"+Cache.size+" "+utils.MAX_CACHE_FULL_SIZE+" "+SSD.writeToSSDQueue.size()+" "+server.removeFromCacheStop);

			try{
//				server.Lock1.lock();
				doWork();
//				server.Lock1.unlock();
			}
			catch(InterruptedException e){
				System.out.println("InterruptedException in removeFromCacheThread: " + e);
			}


			if(server.removeFromCacheStop){
				if(cache.pointersList.size() == 0)
				{
					break;
				}
			}
		}
		System.out.println("removeFromCacheThread ended.");
	}
}
