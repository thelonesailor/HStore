package BlockStorage;

public class cacheValue{
	int pointer;
	boolean dirtyBit; // true-> dirty, false-> nondirty

	public cacheValue(){

	}

	public cacheValue(int pointer, boolean dirtyBit){
		this.pointer = pointer;
		this.dirtyBit = dirtyBit;
	}

	public boolean getDirtyBit(){
		return dirtyBit;
	}

	public void setDirtyBit(boolean dirtyBit){
		this.dirtyBit = dirtyBit;
	}

	public void setPointer(int pointer){
		this.pointer = pointer;
	}

	public int getPointer(){
		return pointer;
	}
}