package BlockStorage;

public class block{
	long blockNumber;
	byte[] blockData;

	Utils utils = new Utils();

	public block(long blockNumber, byte[] blockData){
		this.blockNumber = blockNumber;
		this.blockData = blockData;
	}

	public page readPage(long pageNumber){
		int offset = (int) (pageNumber % utils.BLOCK_SIZE);
		byte[] temp = new byte[utils.PAGE_SIZE];
		System.arraycopy(blockData,offset*utils.PAGE_SIZE,temp,0,utils.PAGE_SIZE);
		return new page(pageNumber, temp);
	}

	public void addPageToBlock(page page){
		int offset = (int) (page.getPageNumber() % 8);
		System.arraycopy(page.getPageData(),0,blockData,offset*utils.PAGE_SIZE,utils.PAGE_SIZE);
	}

	public page[] getAllPages() {
		page[] returnAllPages = new page[utils.BLOCK_SIZE];
		byte[] temp = new byte[utils.PAGE_SIZE];
		for (int i = 0; i < utils.BLOCK_SIZE; i++) {
			System.arraycopy(blockData,i*utils.PAGE_SIZE,temp,0,utils.PAGE_SIZE);
			returnAllPages[i] = new page((blockNumber * 8) + i, temp);
		}
		return returnAllPages;
	}
}