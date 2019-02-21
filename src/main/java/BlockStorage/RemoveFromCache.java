package BlockStorage;

import java.util.Map;

public class RemoveFromCache implements Runnable{
	private cache cache;
	SSD SSD;
	blockServer server;
	private Utils utils;
	private long m1 = -1;

	RemoveFromCache(cache cache, SSD SSD, blockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.utils = utils;
	}

	private void doWork() throws InterruptedException{
		if((cache.cacheList.size() > utils.MAX_CACHE_FULL_SIZE )||( server.removeFromCacheStop && (cache.cacheList.size() > 0))) {
			cacheValue val = new cacheValue();

			cache.cacheListLock.lock();
			cache.cacheList.put(m1,val); // elder gets updated here
			cache.cacheList.remove(m1);
			cache.cacheList.remove(m1);
//			cache.cacheListLock.unlock();

			Map.Entry<Long, cacheValue>  eld = cache.elder;
			long pageNumberToRemove = eld.getKey();
			int pointer = eld.getValue().getPointer();

//			cache.cacheListLock.lock();
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
//			System.out.println("remove from cache!!"+cache.size+" "+utils.MAX_CACHE_FULL_SIZE+" "+SSD.WritetoSSDqueue.size()+" "+server.removeFromCacheStop);

			try{
//				server.Lock1.lock();
				doWork();
//				server.Lock1.unlock();
			}
			catch(InterruptedException e){
				System.out.println("InterruptedException in removeFromCachethread: " + e);
			}


			if(server.removeFromCacheStop){
				if(cache.pointersList.size() == 0)
				{
					break;
				}
			}
		}
		System.out.println("removeFromCachethread ended.");
	}
}
