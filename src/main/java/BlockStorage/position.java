package BlockStorage;

public class position{
	boolean locationCache;
	boolean locationSSD;
	boolean locationHDFS;
	boolean diryBit;

	public position(){

	}

	public position(boolean locationCache, boolean locationSSD, boolean locationHDFS, boolean dirtyBit){
		this.locationCache = locationCache;
		this.locationSSD = locationSSD;
		this.locationHDFS = locationHDFS;
		this.diryBit = dirtyBit;
	}

	public boolean isLocationCache(){
		return locationCache;
	}

	public void setLocationCache(boolean locationCache){
		this.locationCache = locationCache;
	}

	public boolean isLocationHDFS() {
		return locationHDFS;
	}

	public void setLocationHDFS(boolean locationHDFS) {
		this.locationHDFS = locationHDFS;
	}

	public boolean isLocationSSD() {
		return locationSSD;
	}

	public void setLocationSSD(boolean locationSSD) {
		this.locationSSD = locationSSD;
	}

	public boolean isDirty() {
		return diryBit;
	}
}