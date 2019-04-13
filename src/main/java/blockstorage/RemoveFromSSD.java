package blockstorage;

public class RemoveFromSSD implements Runnable {
	Cache cache;
	SSD SSD;
	BlockServer server;
	//	boolean stp;
	private Utils utils;
	int m1 = -1;
	RemoveFromSSD(Cache cache, SSD SSD, BlockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.utils = utils;
	}

	void doWork() throws InterruptedException{
		if((SSD.recencyList.size() > utils.MAX_SSD_FULL_SIZE )
				||( server.removeFromSSDStop && (SSD.recencyList.size() > 0))) {

			SSD.recencyListLock.lock();
			SSD.recencyList.put(m1,false); // elder gets updated here
			SSD.recencyList.remove(m1);
			SSD.recencyList.remove(m1);

//			SSD.recencyListLock.unlock();

			int pageNumberToRemove = blockstorage.SSD.elder.getKey();

//			SSD.recencyListLock.lock();
			SSD.recencyList.remove(pageNumberToRemove);
//			if((int)pageNumberToRemove != -1) {
				SSD.writeToHDFSQueue.add(pageNumberToRemove);
				if(utils.SHOW_LOG)
					System.out.println("Page " + pageNumberToRemove + " added to writeToHDFSQueue");
//			}

			SSD.recencyListLock.unlock();
		}
		else{
			Thread.sleep(10);
		}
	}


	public void run(){

		while (true){
//			System.out.println("remove from SSD!! "+SSD.recencyList.size()+" "+SSD.pointersList.size()+" "+utils.MAX_SSD_FULL_SIZE+" "+SSD.writeToHDFSQueue.size()+" "+server.removeFromSSDStop);

			try{
//				server.Lock2.lock();
				doWork();
//				server.Lock2.unlock();
			}
			catch(InterruptedException e){
				System.out.println("InterruptedException in Remove from SSD thread: " + e);
			}

			if(server.removeFromSSDStop){
				if(!server.writeToSSDThread.isAlive() && SSD.pointersList.size() == 0){
					break;
				}
			}
		}
		System.out.println("Remove from SSD thread ended.");
	}
}
