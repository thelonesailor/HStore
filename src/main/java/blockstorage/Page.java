package blockstorage;

public class Page {
	private int pageNumber;
	private byte[] pageData;

	public Page(int pageNumber, byte[] pageData){
		this.pageNumber = pageNumber;
		this.pageData = pageData;
	}

	public int getPageNumber(){
		return pageNumber;
	}

	public byte[] getPageData(){
		return pageData;
	}
}