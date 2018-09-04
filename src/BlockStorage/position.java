package BlockStorage;

public class position{
    boolean locationCache;
    boolean locationSSD;
    boolean locationHDFS;

    public position(){

    }

    public position(boolean locationCache, boolean locationSSD, boolean locationHDFS){
        this.locationCache = locationCache;
        this.locationSSD = locationSSD;
        this.locationHDFS = locationHDFS;
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
}