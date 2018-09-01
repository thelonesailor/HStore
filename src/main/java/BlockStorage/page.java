package BlockStorage;

public class page{
    private long pageNumber;
    private byte[] pageData;

    public page(long pageNumber, byte[] pageData){
        this.pageNumber = pageNumber;
        this.pageData = pageData;
    }

    public long getPageNumber(){
        return pageNumber;
    }

    public byte[] getPageData(){
        return pageData;
    }
}