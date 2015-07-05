package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.Server;

public class DBCountPageView extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(DBCountPageView.class);
	private Connection connection;
	private boolean initialized;
	private static final String[] AccessFieldNames = { "url", "referrer",
			"time" };
	private static final String[] PageviewFieldNames = { "url", "pageview" };
	private static final String DB_URL = "jdbc:hsqldb:hsql://localhost/URLAccess";
	private static final String DRIVER_CLASS = "org.hsqldb.jdbcDriver";
	private Server server;

	public DBCountPageView() {
		this.initialized = false;
	}

	private void startHsqldbServer() {
		this.server = new Server();
		this.server.setDatabasePath(0,
				System.getProperty("test.build.data", ".") + "/URLAccess");

		this.server.setDatabaseName(0, "URLAccess");
		this.server.start();
	}

	private void createConnection(String driverClassName, String url)
			throws Exception {
		Class.forName(driverClassName);
		this.connection = DriverManager.getConnection(url);
		this.connection.setAutoCommit(false);
	}

	private void shutdown() {
		try {
			this.connection.commit();
			this.connection.close();
		} catch (Throwable ex) {
			LOG.warn("Exception occurred while closing connection :"
					+ StringUtils.stringifyException(ex));
		} finally {
			try {
				if (this.server != null)
					this.server.shutdown();
			} catch (Throwable ex) {
				LOG.warn("Exception occurred while shutting down HSQLDB :"
						+ StringUtils.stringifyException(ex));
			}
		}
	}

	private void initialize(String driverClassName, String url)
			throws Exception {
		if (!this.initialized) {
			if (driverClassName.equals("org.hsqldb.jdbcDriver")) {
				startHsqldbServer();
			}
			createConnection(driverClassName, url);
			dropTables();
			createTables();
			populateAccess();
			this.initialized = true;
		}
	}

	private void dropTables() {
		String dropAccess = "DROP TABLE Access";
		String dropPageview = "DROP TABLE Pageview";
		try {
			Statement st = this.connection.createStatement();
			st.executeUpdate(dropAccess);
			st.executeUpdate(dropPageview);
			this.connection.commit();
			st.close();
		} catch (SQLException ex) {
		}
	}

	private void createTables() throws SQLException {
		String createAccess = "CREATE TABLE Access(url      VARCHAR(100) NOT NULL, referrer VARCHAR(100), time     BIGINT NOT NULL,  PRIMARY KEY (url, time))";

		String createPageview = "CREATE TABLE Pageview(url      VARCHAR(100) NOT NULL, pageview     BIGINT NOT NULL,  PRIMARY KEY (url))";

		Statement st = this.connection.createStatement();
		try {
			st.executeUpdate(createAccess);
			st.executeUpdate(createPageview);
			this.connection.commit();
		} finally {
			st.close();
		}
	}

	private void populateAccess() throws SQLException {
		PreparedStatement statement = null;
		try {
			statement = this.connection
					.prepareStatement("INSERT INTO Access(url, referrer, time) VALUES (?, ?, ?)");

			Random random = new Random();

			int time = random.nextInt(50) + 50;

			int PROBABILITY_PRECISION = 100;
			int NEW_PAGE_PROBABILITY = 15;

			String[] pages = { "/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h",
					"/i", "/j" };

			int[][] linkMatrix = { { 1, 5, 7 }, { 0, 7, 4, 6 }, { 0, 1, 7, 8 },
					{ 0, 2, 4, 6, 7, 9 }, { 0, 1 }, { 0, 3, 5, 9 }, { 0 },
					{ 0, 1, 3 }, { 0, 2, 6 }, { 0, 2, 6 } };

			int currentPage = random.nextInt(pages.length);
			String referrer = null;

			for (int i = 0; i < time; i++) {
				statement.setString(1, pages[currentPage]);
				statement.setString(2, referrer);
				statement.setLong(3, i);
				statement.execute();

				int action = random.nextInt(100);

				if (action < 15) {
					currentPage = random.nextInt(pages.length);
					referrer = null;
				} else {
					referrer = pages[currentPage];
					action = random.nextInt(linkMatrix[currentPage].length);
					currentPage = linkMatrix[currentPage][action];
				}
			}

			this.connection.commit();
		} catch (SQLException ex) {
			this.connection.rollback();
			throw ex;
		} finally {
			if (statement != null)
				statement.close();
		}
	}

	private boolean verify() throws SQLException {
		String countAccessQuery = "SELECT COUNT(*) FROM Access";
		String sumPageviewQuery = "SELECT SUM(pageview) FROM Pageview";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = this.connection.createStatement();
			rs = st.executeQuery(countAccessQuery);
			rs.next();
			long totalPageview = rs.getLong(1);

			rs = st.executeQuery(sumPageviewQuery);
			rs.next();
			long sumPageview = rs.getLong(1);

			LOG.info("totalPageview=" + totalPageview);
			LOG.info("sumPageview=" + sumPageview);

			return (totalPageview == sumPageview) && (totalPageview != 0L);
		} finally {
			if (st != null)
				st.close();
			if (rs != null)
				rs.close();
		}
	}

	public int run(String[] args) throws Exception {
		String driverClassName = "org.hsqldb.jdbcDriver";
		String url = "jdbc:hsqldb:hsql://localhost/URLAccess";

		if (args.length > 1) {
			driverClassName = args[0];
			url = args[1];
		}

		initialize(driverClassName, url);

		JobConf job = new JobConf(getConf(), DBCountPageView.class);

		job.setJobName("Count Pageviews of URLs");

		job.setMapperClass(PageviewMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(PageviewReducer.class);

		DBConfiguration.configureDB(job, driverClassName, url);

		DBInputFormat.setInput(job, AccessRecord.class, "Access", null, "url",
				AccessFieldNames);

		DBOutputFormat.setOutput(job, "Pageview", PageviewFieldNames);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(PageviewRecord.class);
		job.setOutputValueClass(NullWritable.class);
		try {
			JobClient.runJob(job);

			boolean correct = verify();
			if (!correct)
				throw new RuntimeException("Evaluation was not correct!");
		} finally {
			shutdown();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new DBCountPageView(), args);
		System.exit(ret);
	}

	static class PageviewReducer extends MapReduceBase
			implements
			Reducer<Text, LongWritable, DBCountPageView.PageviewRecord, NullWritable> {
		NullWritable n = NullWritable.get();

		public void reduce(
				Text key,
				Iterator<LongWritable> values,
				OutputCollector<DBCountPageView.PageviewRecord, NullWritable> output,
				Reporter reporter) throws IOException {
			long sum = 0L;
			while (values.hasNext()) {
				sum += ((LongWritable) values.next()).get();
			}
			output.collect(new DBCountPageView.PageviewRecord(key.toString(),
					sum), this.n);
		}
	}

	static class PageviewMapper extends MapReduceBase
			implements
			Mapper<LongWritable, DBCountPageView.AccessRecord, Text, LongWritable> {
		LongWritable ONE = new LongWritable(1L);

		public void map(LongWritable key, DBCountPageView.AccessRecord value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			Text oKey = new Text(value.url);
			output.collect(oKey, this.ONE);
		}
	}

	static class PageviewRecord implements Writable, DBWritable {
		String url;
		long pageview;

		public PageviewRecord(String url, long pageview) {
			this.url = url;
			this.pageview = pageview;
		}

		public void readFields(DataInput in) throws IOException {
			this.url = Text.readString(in);
			this.pageview = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			Text.writeString(out, this.url);
			out.writeLong(this.pageview);
		}

		public void readFields(ResultSet resultSet) throws SQLException {
			this.url = resultSet.getString(1);
			this.pageview = resultSet.getLong(2);
		}

		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.url);
			statement.setLong(2, this.pageview);
		}

		public String toString() {
			return this.url + " " + this.pageview;
		}
	}

	static class AccessRecord implements Writable, DBWritable {
		String url;
		String referrer;
		long time;

		public void readFields(DataInput in) throws IOException {
			this.url = Text.readString(in);
			this.referrer = Text.readString(in);
			this.time = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			Text.writeString(out, this.url);
			Text.writeString(out, this.referrer);
			out.writeLong(this.time);
		}

		public void readFields(ResultSet resultSet) throws SQLException {
			this.url = resultSet.getString(1);
			this.referrer = resultSet.getString(2);
			this.time = resultSet.getLong(3);
		}

		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.url);
			statement.setString(2, this.referrer);
			statement.setLong(3, this.time);
		}
	}
}
