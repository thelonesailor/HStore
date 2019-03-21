package BlockStorage;

public class PageIndex {
	position[] pageIndex;

	PageIndex(){
		pageIndex = new position[4000];
	}

	synchronized void updatePageIndex(int pageNumber,int Cache, int SSD, int HDFS, int dirty){
		boolean locationCache, locationSSD, locationHDFS, dirtyBit;

//		long mask = (1<<31 - 1);
//		int pageNumber = (int)(pageNumberLong & mask); //take last 30 bits
//		System.out.println(pageNumber + " " + pageIndex[pageNumber]);

		if(pageIndex[pageNumber] != null) {
			position po = pageIndex[pageNumber];

			if (Cache == 1) locationCache = true;
			else if (Cache == 0) locationCache = false;
			else locationCache = po.locationCache;

			if (SSD == 1) locationSSD = true;
			else if (SSD == 0) locationSSD = false;
			else locationSSD = po.locationSSD;

			if (HDFS == 1) locationHDFS = true;
			else if (HDFS == 0) locationHDFS = false;
			else locationHDFS = po.locationHDFS;

			if (dirty == 1) dirtyBit = true;
			else if (dirty == 0) dirtyBit = false;
			else dirtyBit = po.diryBit;

			pageIndex[pageNumber] = new position(locationCache, locationSSD, locationHDFS, dirtyBit);
		}
		else {

			if (Cache == 1) locationCache = true;
			else if (Cache == 0) locationCache = false;
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
//				assert false;
				return;
			}

			if (SSD == 1) locationSSD = true;
			else if (SSD == 0) locationSSD = false;
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
//				assert false;
				return;
			}

			if (HDFS == 1) locationHDFS = true;
			else if (HDFS == 0) locationHDFS = false;
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
//				assert false;
				return;
			}

			if (dirty == 1) dirtyBit = true;
			else if (dirty == 0) dirtyBit = false;
			else {
				System.out.println("Error in updating pageIndex for "+pageNumber);
//				assert false;
				return;
			}

			pageIndex[pageNumber] = new position(locationCache, locationSSD, locationHDFS, dirtyBit);
		}
	}

}
