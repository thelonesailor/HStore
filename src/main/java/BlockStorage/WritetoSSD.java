package BlockStorage;

import javafx.util.Pair;

import java.io.FileOutputStream;
import java.io.IOException;

// Assumption: all writes to SSD happen from Cache
public class WritetoSSD implements Runnable{
	private cache cache;
	private SSD SSD;
	private blockServer server;
	private Utils utils;
	String SSD_LOCATION;


	WritetoSSD(cache cache,SSD SSD, blockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.utils = utils;
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}

	void doWork() throws InterruptedException{
		if(SSD.WritetoSSDqueue.size() > 0) {
			Pair<Long, Integer> p = SSD.WritetoSSDqueue.remove();
			long pageNumber = p.getKey();
			if(!server.pageIndex.get(pageNumber).isLocationCache()){
				return;
			}
//			System.out.println(pageNumber);
			String fileName = SSD_LOCATION + "/" + pageNumber;

			try {
				FileOutputStream out = new FileOutputStream(fileName);
//				cache.cacheList.get(pageNumber);
//				System.out.println("Writing "+pageNumber+" to SSD.");
				int freePointer = p.getValue();
				out.write(cache.readPage(pageNumber, true).getPageData());
				out.close();
//				System.out.println("Wrote "+pageNumber+" to SSD.");

				server.updatePageIndex(pageNumber, 0, 1, -1, -1);
//				cache.cacheList.remove(pageNumber);
				cache.pointersList.remove(pageNumber);
				cache.EmptyPointers.add(freePointer);
				cache.size.getAndDecrement();// cache.size--

				SSD.recencyListLock.lock();
				SSD.recencyList.put(pageNumber, true); //elder is updated
				SSD.recencyListLock.unlock();

				if(!SSD.pointersList.contains(pageNumber)){
					SSD.pointersList.add(pageNumber);
					SSD.size.getAndIncrement();
				}
			} catch (IOException e) {
				System.out.println("Exception Occurred:");
				e.printStackTrace();
			}
		}
		else{
			Thread.sleep(30);
		}
	}

	public void run(){
		System.out.println("writetoSSDthread started.");
		while (true) {
			try{
				doWork();
			}
			catch(InterruptedException e){
				System.out.println("InterruptedException in Write to SSD thread: " + e);
			}
//			System.out.println("WritetoSSDqueue.size() & cache.size & cache.cachelist.size = " + SSD.WritetoSSDqueue.size()+" "+cache.size.get()+" "+cache.cacheList.size()+" "+server.writetoSSDStop);

			if(server.writetoSSDStop){
				if(!server.removeFromCachethread.isAlive() && SSD.WritetoSSDqueue.size() == 0){
					break;
				}
			}
		}
		System.out.println("writetoSSDthread ended.");
	}
}

