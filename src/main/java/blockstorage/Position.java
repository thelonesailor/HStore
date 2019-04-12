package blockstorage;

public class Position {
	boolean locationCache;
	boolean locationSSD;
	boolean locationHDFS;
	boolean diryBit;
	boolean present = false;    //tells Page is in pageindex or not

	public Position(boolean locationCache, boolean locationSSD, boolean locationHDFS, boolean dirtyBit){
		this.locationCache = locationCache;
		this.locationSSD = locationSSD;
		this.locationHDFS = locationHDFS;
		this.diryBit = dirtyBit;
		this.present = true;
	}

	public Position(){
		this.present = false;
	}

	boolean isLocationCache(){
		return locationCache;
	}

//	public void setLocationCache(boolean locationCache){
//		this.locationCache = locationCache;
//	}

	boolean isLocationHDFS() {
		return locationHDFS;
	}

//	public void setLocationHDFS(boolean locationHDFS) {
//		this.locationHDFS = locationHDFS;
//	}

	boolean isLocationSSD() {
		return locationSSD;
	}

//	public void setLocationSSD(boolean locationSSD) {
//		this.locationSSD = locationSSD;
//	}

	boolean isDirty() {
		return diryBit;
	}
}