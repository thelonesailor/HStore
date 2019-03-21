package BlockStorage;

public class page{
	private int pageNumber;
	private byte[] pageData;

	public page(int pageNumber, byte[] pageData){
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