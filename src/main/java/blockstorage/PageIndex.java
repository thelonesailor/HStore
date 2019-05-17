package blockstorage;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class PageIndex {
//	private Position[] pageIndex;
	List<Position[]> pageIndex;
	int localPageNumberMask = (1<<25) - 1;
	int VMIDmask = ((1<<30) - 1) - localPageNumberMask;
	BlockServer server;

	PageIndex(BlockServer server){
		this.server = server;
		pageIndex = new ArrayList<>();
	}

	void addVM(int pagesWanted){
		pageIndex.add(new Position[pagesWanted]);
	}

	Position get(int pageNumber){
		int localPageNumber = pageNumber & localPageNumberMask;
		int VMID = (pageNumber & VMIDmask)>>25;
		return (pageIndex.get(VMID)[localPageNumber]);
	}

	void writeToFilePageIndex(){
		try {
			PrintWriter out = new PrintWriter("/var/www/html/data/" +
					"pageIndex.txt");
			for(int VMID=0;VMID<pageIndex.size();++VMID){
				Position[] positions = pageIndex.get(VMID);
				for(int localPageNumber=0;localPageNumber<positions.length;++localPageNumber){
					int pageNumber = (VMID<<25)|localPageNumber;
					Position position = positions[localPageNumber];

					if(position != null){
						out.println(pageNumber);
						if(position.locationCache){
							out.print("1 ");
						}
						else{
							out.print("0 ");
						}
						if(position.locationSSD){
							out.print("1 ");
						}
						else{
							out.print("0 ");
						}
						if(position.locationHDFS){
							out.println("1");
						}
						else{
							out.println("0");
						}
					}
				}
			}
			out.close();
		}catch(Exception e){e.printStackTrace();}
	}

	synchronized void updatePageIndex(int pageNumber,int Cache, int SSD, int HDFS, int dirty){
		boolean locationCache, locationSSD, locationHDFS, dirtyBit;

		int localPageNumber = pageNumber & localPageNumberMask;
		int VMID = (pageNumber & VMIDmask)>>25;
		if(VMID >= pageIndex.size()){
			server.vMmanager.registerVM(10000);
		}
//		System.out.println("Updating pageIndex for VMID="+VMID+" localPageNumber="+localPageNumber);
		if(pageIndex.get(VMID)[localPageNumber] != null) {
			Position po = pageIndex.get(VMID)[localPageNumber];

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

			pageIndex.get(VMID)[localPageNumber] = new Position(locationCache, locationSSD, locationHDFS, dirtyBit);
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

			pageIndex.get(VMID)[localPageNumber] = new Position(locationCache, locationSSD, locationHDFS, dirtyBit);
		}
	}

}
