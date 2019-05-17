package blockstorage;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class VMManager {
	BlockServer server;
	int VMIDMax = 0;
	@NotNull List<Integer> pagesForEachVM = new ArrayList<>();

	VMManager(BlockServer server){
		this.server = server;

		VMIDMax = 0;
	}

	int registerVM(int numPages){
		pagesForEachVM.add(numPages);
		server.pageIndex.addVM(numPages);
		return VMIDMax++;
	}
}
