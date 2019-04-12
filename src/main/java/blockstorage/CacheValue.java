package blockstorage;

public class CacheValue {
	int pointer;

	public CacheValue(){

	}

	public CacheValue(int pointer){
		this.pointer = pointer;
	}

	public void setPointer(int pointer){
		this.pointer = pointer;
	}

	public int getPointer(){
		return pointer;
	}
}