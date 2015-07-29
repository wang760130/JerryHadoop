package com.jerry.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author Wangjiajun
 * @date 2015年7月15日
 */
public class HdfsDao {
	
	private static final String HDFS = "hdfs://10.58.28.85:9000/";
	private String hdfsPath = null;
	private Configuration conf = null;

	public HdfsDao(Configuration conf) {
		this(HDFS, conf);
	}

	public HdfsDao(String hdfsPath, Configuration conf) {
		this.hdfsPath = hdfsPath;
		this.conf = conf;
	}

	public static JobConf config() {
		JobConf conf = new JobConf(HdfsDao.class);
		conf.setJobName("HdfsDAO");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}

	public void mkdirs(String folder) throws IOException {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			Path path = new Path(folder);

			if (!fs.exists(path)) {
				fs.mkdirs(path);
				System.out.println("create " + folder + " success...");
			} else {
				System.out.println(folder + "is exists...");
			}

		} catch (IOException e) {
			System.out.println("create " + folder + "exception...");
			throw e;
		} finally {
			if(fs != null)
				fs.close();
		}

	}

	public void rmr(String folder) throws IOException {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			Path path = new Path(folder);
			
			fs.deleteOnExit(path);
			System.out.println("delete " + folder + " success...");
		} catch (IOException e) {
			System.out.println("delete " + folder + " exception...");
			throw e;
		} finally {
			if(fs != null)
				fs.close();
		}
		
	}

	public FileStatus[] ls(String folder) throws IOException {
		FileSystem fs = null;
		FileStatus[] list = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			Path path = new Path(folder);
			list = fs.listStatus(path);
			System.out.println("list " + folder + " success...");
		} catch (IOException e) {
			System.out.println("list " + folder + " exception...");
			throw e;
		} finally {
			if(fs != null)
				fs.close();
		}
		return list;
	}

	public void createFile(String file, String content) throws IOException {
		FileSystem fs = null;
		FSDataOutputStream os = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			os = fs.create(new Path(file));
			if(null != content) {
				byte[] buff = content.getBytes();
				os.write(buff, 0, buff.length);
			}
			System.out.println("create " + file + " success...");
		} catch (IOException e) {
			System.out.println("create " + file + " exception...");
			throw e;
		} finally {
			if (os != null)
				os.close();
			if(fs != null)
				fs.close();
		}
	}

	public void copyFile(String local, String remote) throws IOException {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			fs.copyFromLocalFile(new Path(local), new Path(remote));
			System.out.println("copy from " + local + " to " + remote + " success...");
		} catch (IOException e) {
			System.out.println("copy from " + local + " to " + remote + " exception...");
			throw e;
		} finally {
			if(fs != null)
				fs.close();
		}
	}

	public void download(String remote, String local) throws IOException {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			Path path = new Path(remote);
			
			fs.copyToLocalFile(path, new Path(local));
			System.out.println("download from" + remote + " to " + local + "success...");
		} catch(IOException e) {
			System.out.println("download from" + remote + " to " + local + "exception...");
			throw e;
		} finally {
			if(fs != null)
				fs.close();
		}
	}

	public void cat(String remoteFile) throws IOException {
		FileSystem fs = null;
		FSDataInputStream fsdis = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			Path path = new Path(remoteFile);
			fsdis = fs.open(path);
			System.out.println("cat" + remoteFile + " success...");
			IOUtils.copyBytes(fsdis, System.out, 4096, false);
		} catch(IOException e) {
			System.out.println("cat" + remoteFile + " exception...");
			throw e;
		} finally {
			IOUtils.closeStream(fsdis);
			if(fs != null)
				fs.close();
		}
	}
}
