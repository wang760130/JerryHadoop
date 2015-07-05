package com.jerry.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS操作类
 * @author Jerry Wang
 */
public class HDFSUtil {
	private static final String HDFS_PATH = "hdfs://192.168.23.132:9000";
	private static final String DIR_PATH = "/jerry";
	
	private static FileSystem fileSystem = null;
	
	static {
		try {
			fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 创建文件夹
	 * @param dir
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static boolean mkdirs(String dir) throws IOException {
		return fileSystem.mkdirs(new Path(dir));
	}
	
	/**
	 * 上传文件
	 * @param filename
	 * @return
	 * @throws IOException 
	 */
	public static void upload(File file) throws IOException {
		final FSDataOutputStream out = fileSystem.create(new Path(DIR_PATH + "/" + file.getName()));
		final InputStream in = new FileInputStream(file);
		IOUtils.copyBytes(in, out, 100, true);
	}
	
	/**
	 * 下载文件
	 * @throws IOException 
	 */
	public static void download(String file) throws IOException {
		FSDataInputStream  in = fileSystem.open(new Path(DIR_PATH + "/" + file));
		IOUtils.copyBytes(in, System.out, 1024, true); 
	}
	
	/**
	 * 删除文件（夹）
	 * @throws IOException 
	 */
	public static boolean delete(String file) throws IOException {
		return fileSystem.delete(new Path("/" + file), true);
	}
	
	/**
	 * 读取文件
	 * @throws IOException 
	 */
	public static void get() throws IOException {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL("hdfs://192.168.23.130:9000/jerry/a.log");
		final InputStream in = url.openStream();
		IOUtils.copyBytes(in, System.out, 1024, true); 
	}
	
	/**
	 * 获取HDFS节点
	 * @return
	 * @throws IOException
	 */
	public static DatanodeInfo[] getHDFSNodes() throws IOException {
		DistributedFileSystem hdfs = (DistributedFileSystem)fileSystem;
		DatanodeInfo[] dataNodeStates = hdfs.getDataNodeStats();
		return dataNodeStates;
	}
	
}
