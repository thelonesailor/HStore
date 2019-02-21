package BlockStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Simple Driver to read/write to hdfs
 *
 */
public class FileSystemOperations {
	Utils utils;
	public FileSystemOperations(Utils utils) {
		this.utils = utils;
	}

	void closeFS(Configuration conf){
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			fileSystem.close();
		} catch (IOException e) {
			System.out.println("Error while closing filesystem");
		}
	}

	public  Configuration getConfiguration(){
		Configuration config = new Configuration();
		System.setProperty("HADOOP_USER_NAME", utils.HADOOP_USER_NAME);

		String HADOOP_HOME=System.getenv("HADOOP_HOME");
//		String HADOOP_HOME="/usr/local/hadoop";
//		System.out.println(System.getenv("HADOOP_HOME"));
		config.addResource(new Path(HADOOP_HOME+"/etc/hadoop/core-site.xml"));
		config.addResource(new Path(HADOOP_HOME+"/etc/hadoop/hdfs-site.xml"));
//		config.set("fs.defaultFS", FSNAME);
//		config.setInt("dfs.replication", 2);
		return config;
	}


	/**
	 * create a existing file from local filesystem to hdfs
	 * @param block
	 * @param conf
	 * @throws IOException
	 */
	synchronized public void addFile(Configuration conf, block block) throws IOException {

	FileSystem fileSystem;

	String file = conf.get("fs.defaultFS")+utils.HDFS_PATH+"/"+(block.blockNumber);
//	System.out.println(file);

	Path path = new Path(file);
//	if (fileSystem.exists(path)) {
//
//		// delete the old file and write new file.
//		deleteFile(block.blockNumber,conf);
//	}

	fileSystem = FileSystem.get(conf);

	// Create a new file and write data to it.
	FSDataOutputStream out = fileSystem.create(path);
	//InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));

	// convert block to byte array
	int numBytes = utils.PAGE_SIZE;

	out.write(block.blockData, 0, 8*numBytes);

	// Close all the file descriptors
	out.close();
//	fileSystem.close();
  }

  /**
   * read a file from hdfs
   * @param blockNumber
   * @param conf
   * @throws IOException
   */
	synchronized public byte[] readFile( Configuration conf,long blockNumber) throws IOException {
	  
	FileSystem fileSystem = FileSystem.get(conf);

	String file = conf.get("fs.defaultFS")+utils.HDFS_PATH+"/"+Long.toString(blockNumber);
//	System.out.println(file);

	Path path = new Path(file);
	byte[] b = new byte[8*utils.PAGE_SIZE];
	if (!fileSystem.exists(path)) {
	   System.out.println("File " + file + " does not exist");
//	  fileSystem.close();
	  return b;
	}

	FSDataInputStream in = fileSystem.open(path);


	int numBytes = 0;
	while ((numBytes = in.read(b)) > 0) {
		in.close();
//		fileSystem.close();
		return b;
	}
	in.close();
//	fileSystem.close();
	return b;
	}

  /**
   * delete a directory in hdfs
   * @param blockNumber
   * @throws IOException
   */
	public void deleteFile(long blockNumber, Configuration conf) throws IOException {
	FileSystem fileSystem = FileSystem.get(conf);

	String file = conf.get("fs.defaultFS")+utils.HDFS_PATH+"/"+Long.toString(blockNumber);
//	System.out.println(file);

		Path path = new Path(file);
	if (!fileSystem.exists(path)) {
		System.out.println("File " + file + " does not exist");
//		fileSystem.close();
		return;
	}

	fileSystem.delete(new Path(file), true);

//	fileSystem.close();
	}

	/**
	* create directory in hdfs
	* @param dir
	* @throws IOException
	*/
	public void mkdir(String dir, Configuration conf) throws IOException {
	FileSystem fileSystem = FileSystem.get(conf);

	Path path = new Path(dir);
	if (fileSystem.exists(path)) {
		System.out.println("Dir " + dir + " already not exists");
		return;
	}

	fileSystem.mkdirs(path);

//	fileSystem.close();
	}

//  public static void main(String[] args) throws IOException {
//
//    if (args.length < 1) {
//      System.out.println("Usage: hdfsclient add/read/delete/mkdir"
//          + " [<local_path> <hdfs_path>]");
//      System.exit(1);
//    }
//
//    FileSystemOperations client = new FileSystemOperations();
//    //String hdfsPath = "hdfs://" + args[0] + ":" + args[1];
//
//    
//    Configuration conf = new Configuration();
//    System.setProperty("HADOOP_USER_NAME", "hduser");
//    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
//    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
//    // Providing conf files
//    // conf.addResource(new Path(HDFSAPIDemo.class.getResource("/conf/core-site.xml").getFile()));
//    // conf.addResource(new Path(HDFSAPIDemo.class.getResource("/conf/hdfs-site.xml").getFile()));
//    // (or) using relative paths
//    //    conf.addResource(new Path(
//    //        "/u/hadoop-1.0.2/conf/core-site.xml"));
//    //    conf.addResource(new Path(
//    //        "/u/hadoop-1.0.2/conf/hdfs-site.xml"));
//
//    //(or)
//    // alternatively provide namenode host and port info
//    
//    //conf.set("fs.default.name", hdfsPath);
//
//    if (args[0].equals("add")) {
//      if (args.length < 3) {
//        System.out.println("Usage: hdfsclient add <local_path> "
//            + "<hdfs_path>");
//        System.exit(1);
//      }
//
//      client.addFile(args[1], args[2], conf);
//
//    } else if (args[0].equals("read")) {
//      if (args.length < 2) {
//        System.out.println("Usage: hdfsclient read <hdfs_path>");
//        System.exit(1);
//      }
//
//      client.readFile(args[1], conf);
//
//    } else if (args[0].equals("delete")) {
//      if (args.length < 2) {
//        System.out.println("Usage: hdfsclient delete <hdfs_path>");
//        System.exit(1);
//      }
//
//      client.deleteFile(args[1], conf);
//
//    } else if (args[0].equals("mkdir")) {
//      if (args.length < 2) {
//        System.out.println("Usage: hdfsclient mkdir <hdfs_path>");
//        System.exit(1);
//      }
//
//      client.mkdir(args[1], conf);
//
//    } else {
//      System.out.println("Usage: hdfsclient add/read/delete/mkdir"
//          + " [<local_path> <hdfs_path>]");
//      System.exit(1);
//    }
//
//    System.out.println("Done!");
//  }
}
