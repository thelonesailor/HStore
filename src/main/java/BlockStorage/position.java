package BlockStorage;

public class position{
	boolean locationCache;
	boolean locationSSD;
	boolean locationHDFS;
	boolean diryBit;
	boolean present = false;    //tells page is in pageindex or not

	public position(boolean locationCache, boolean locationSSD, boolean locationHDFS, boolean dirtyBit){
		this.locationCache = locationCache;
		this.locationSSD = locationSSD;
		this.locationHDFS = locationHDFS;
		this.diryBit = dirtyBit;
		this.present = true;
	}

	public position(){
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