package blockstorage;

public class BlockValue {
	int pointer;
	boolean dirtyBit; // true-> dirty, false-> nondirty

	public BlockValue(){

	}

	public BlockValue(int pointer, boolean dirtyBit){
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