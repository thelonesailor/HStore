package blockstorage;

import javafx.util.Pair;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

// Assumption: all writes to SSD happen from Cache
public class WriteToSSD implements Runnable{
	private Cache cache;
	private SSD SSD;
	private BlockServer server;
	private Utils utils;
	String SSD_LOCATION;


	WriteToSSD(Cache cache, SSD SSD, BlockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.utils = utils;
		this.SSD_LOCATION = utils.getSSD_LOCATION();
	}

	void doWork() throws InterruptedException{
		if(SSD.writeToSSDQueue.size() > 0) {
			Pair<Integer, Integer> p = SSD.writeToSSDQueue.remove();
			int pageNumber = p.getKey();
//			if(!server.pageIndex.get(pageNumber).isLocationCache()){
//				return;
//			}
//			System.out.println(pageNumber);
			String fileName = SSD_LOCATION + "/" + pageNumber;
			File file = new File(fileName);

			while (SSD.pointersList.size() >= utils.SSD_SIZE){} // wait for SSD to have space

			try {
				file.createNewFile();
			}catch (IOException e){}

			try {
				FileOutputStream out = new FileOutputStream(fileName);
//				Cache.cacheList.get(pageNumber);
//				System.out.println("Writing "+pageNumber+" to SSD.");
				int freePointer = p.getValue();
				out.write(cache.readPage(pageNumber, true).getPageData());
				out.close();
//				System.out.println("Wrote "+pageNumber+" to SSD.");

				server.pageIndex.updatePageIndex(pageNumber, 0, 1, -1, -1);
//				Cache.cacheList.remove(pageNumber);
				cache.pointersList.remove(pageNumber);
				cache.EmptyPointers.add(freePointer);
//				Cache.size.getAndDecrement();// Cache.size--

				SSD.recencyListLock.lock();
				SSD.recencyList.put(pageNumber, true); //elder is updated
				SSD.recencyListLock.unlock();

				SSD.pointersList.add(pageNumber);

//				if((int)pageNumber == 50) {
//					int lmaolmao=23;
//				}
			} catch (IOException e) {
				System.out.println("Exception Occurred:");
				e.printStackTrace();
			}
		}
		else{
			Thread.sleep(10);
		}
	}

	public void run(){

		while (true) {
			try{
//				server.Lock1.lock();
				doWork();
//				server.Lock1.unlock();
			}
			catch(InterruptedException e){
				System.out.println("InterruptedException in Write to SSD thread: " + e);
			}
//			System.out.println("writeToSSDQueue.size()="+SSD.writeToSSDQueue.size()+"  Cache.pointersList.size()="+Cache.pointersList.size()+"  Cache.cacheList.size()="+Cache.cacheList.size()+"  SSD.recencyList.size()="+SSD.recencyList.size()+"  server.writeToSSDStop="+server.writeToSSDStop);


			if(server.writeToSSDStop){
				if(!server.removeFromCacheThread.isAlive() && SSD.writeToSSDQueue.size() == 0){
					break;
				}
			}
		}
		System.out.println("writetoSSDthread ended.");
	}
}
