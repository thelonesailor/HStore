package blockstorage;

import org.jetbrains.annotations.NotNull;

class Block {
	int blockNumber;
	byte[] blockData;

	Utils utils;

	Block(int blockNumber, byte[] blockData, Utils utils){
		this.blockNumber = blockNumber;
		this.blockData = blockData;
		this.utils = utils;
	}

	@NotNull Page readPage(int pageNumber){
		int offset = (pageNumber % utils.BLOCK_SIZE);
		byte[] temp = new byte[utils.PAGE_SIZE];
		System.arraycopy(blockData,offset*utils.PAGE_SIZE,temp,0,utils.PAGE_SIZE);
		return new Page(pageNumber, temp);
	}

	void addPageToBlock(@NotNull Page page){
		int offset = (page.getPageNumber() % 8);
		System.arraycopy(page.getPageData(),0,blockData,offset*utils.PAGE_SIZE,utils.PAGE_SIZE);
	}

	@NotNull Page[] getAllPages() {
		Page[] returnAllPages = new Page[utils.BLOCK_SIZE];
		byte[] temp = new byte[utils.PAGE_SIZE];
		for (int i = 0; i < utils.BLOCK_SIZE; i++) {
			System.arraycopy(blockData,i*utils.PAGE_SIZE,temp,0,utils.PAGE_SIZE);
			returnAllPages[i] = new Page((blockNumber << 3) + i, temp);
		}
		return returnAllPages;
	}
}