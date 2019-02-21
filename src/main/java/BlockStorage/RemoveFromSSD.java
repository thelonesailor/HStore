package BlockStorage;

public class RemoveFromSSD implements Runnable {
	cache cache;
	SSD SSD;
	blockServer server;
	//	boolean stp;
	private Utils utils;
	long m1 = -1;
	RemoveFromSSD(cache cache, SSD SSD, blockServer server, Utils utils){
		this.cache = cache;
		this.SSD = SSD;
		this.server = server;
		this.utils = utils;
	}

	void doWork() throws InterruptedException{
		if((SSD.recencyList.size() > utils.MAX_SSD_FULL_SIZE )||( server.removeFromSSDStop && (SSD.recencyList.size() > 0))) {

			SSD.recencyListLock.lock();
			SSD.recencyList.put(m1,false); // elder gets updated here
			SSD.recencyList.remove(m1);
			SSD.recencyList.remove(m1);

//			SSD.recencyListLock.unlock();

			long pageNumberToRemove = SSD.elder.getKey();

//			SSD.recencyListLock.lock();
			SSD.recencyList.remove(pageNumberToRemove);
			SSD.recencyListLock.unlock();
//			if((int)pageNumberToRemove != -1) {
				SSD.WritetoHDFSqueue.add(pageNumberToRemove);
				if(utils.SHOW_LOG)
					System.out.println("page " + pageNumberToRemove + " added to WritetoHDFSqueue");
//			}
		}
		else{
			Thread.sleep(10);
		}
	}


	public void run(){

		while (true){
//			System.out.println("remove from SSD!! "+SSD.recencyList.size()+" "+SSD.size.get()+" "+utils.MAX_SSD_FULL_SIZE+" "+SSD.WritetoHDFSqueue.size()+" "+server.removeFromSSDStop);

			try{
//				server.Lock2.lock();
				doWork();
//				server.Lock2.unlock();
			}
			catch(InterruptedException e){
				System.out.println("InterruptedException in Remove from SSD thread: " + e);
			}

			if(server.removeFromSSDStop){
				if(!server.writeToSSDthread.isAlive() && SSD.pointersList.size() == 0){
					break;
				}
			}
		}
		System.out.println("Remove from SSD thread ended.");
	}
}
