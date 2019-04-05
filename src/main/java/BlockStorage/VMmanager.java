package BlockStorage;

import java.util.ArrayList;
import java.util.List;

public class VMmanager {
	blockServer server;
	int VMIDMax = 0;
	List<Integer> pagesForEachVM = new ArrayList<>();

	VMmanager(blockServer server){
		this.server = server;

		VMIDMax = 0;
	}

	int registerVM(int numPages){
		pagesForEachVM.add(numPages);
		server.pageIndex.addVM(VMIDMax,numPages);
		return VMIDMax++;
	}
}
