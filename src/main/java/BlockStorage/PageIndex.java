package BlockStorage;

import java.util.ArrayList;
import java.util.List;

public class PageIndex {
//	private position[] pageIndex;
	List<position[]> pageIndex ;
	int localPageNumberMask = (1<<26) - 1;
	int VMIDmask = ((1<<31) - 1) - localPageNumberMask;

	PageIndex(){
		pageIndex = new ArrayList<>();
	}

	void addVM(int VMID,int pagesWanted){
		pageIndex.add(new position[pagesWanted]);
	}

	position get(int pageNumber){
		int localPageNumber = pageNumber & localPageNumberMask;
		int VMID = pageNumber & VMIDmask;
		return (pageIndex.get(VMID)[localPageNumber]);
	}

	synchronized void updatePageIndex(int pageNumber,int Cache, int SSD, int HDFS, int dirty){
		boolean locationCache, locationSSD, locationHDFS, dirtyBit;

		int localPageNumber = pageNumber & localPageNumberMask;
		int VMID = pageNumber & VMIDmask;

		if(pageIndex.get(VMID)[localPageNumber] != null) {
			position po = pageIndex.get(VMID)[localPageNumber];

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

			pageIndex.get(VMID)[localPageNumber] = new position(locationCache, locationSSD, locationHDFS, dirtyBit);
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

			pageIndex.get(VMID)[localPageNumber] = new position(locationCache, locationSSD, locationHDFS, dirtyBit);
		}
	}

}
