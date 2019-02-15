package BlockStorage;

public class RemoveFromCache implements Runnable{
	cache cache;
	SSD SSD;
	blockServer server;
//	boolean stp;
	private Utils utils;
	long m1 = -1;
	RemoveFromCache(cache cache, SSD SSD, blockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.utils = utils;
	}

	public void doWork() throws InterruptedException{
		if((cache.cacheList.size() > utils.MAX_CACHE_FULL_SIZE )||( server.removeFromCacheStop && (cache.cacheList.size() > 0))) {
			cacheValue val = new cacheValue();

			cache.cacheListLock.lock();
			cache.cacheList.put(m1,val); // elder gets updated here
			cache.cacheList.remove(m1);
			cache.cacheListLock.unlock();

			long pageNumberToRemove = cache.elder.getKey();
			int pointer = cache.elder.getValue().getPointer();

			cache.cacheListLock.lock();
			cache.cacheList.remove(pageNumberToRemove);
			cache.cacheListLock.unlock();


			SSD.writePage(pageNumberToRemove, pointer, server);

//			if(Thread.currentThread().isInterrupted()){
//				throw new InterruptedException();
//				stp = true;
//			}
		}
		else{
			Thread.sleep(10);
		}
	}
	public void run(){
		while (true) {
//			System.out.println("remove from cache!!"+cache.size+" "+utils.MAX_CACHE_FULL_SIZE+" "+SSD.WritetoSSDqueue.size()+" "+server.removeFromCacheStop);

			try{
				doWork();
			}
			catch(InterruptedException e){
//				stp = true;
//				System.out.println("stp = "+stp);
			}

//			try{Thread.sleep(1);}
//			catch(InterruptedException e){stop = true;}

//			if(Thread.currentThread().isInterrupted()){
//				stp = true;
//			}

			if(server.removeFromCacheStop){
				if(cache.size.get() == 0)
				{
					break;
				}
			}
		}

		System.out.println("Remove from Cache thread ended.");
	}
}
