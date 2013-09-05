package lv.edu.linux.hadoop.warc.jobs;

import edu.umd.cloud9.collection.clue.ClueWarcInputFormat;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import java.io.*;
import java.util.Iterator;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class DomainPageContentTypeCounter {

	public static class DomainPageContentTypeCounterMapper extends MapReduceBase implements Mapper<Writable, ClueWarcRecord, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static Pattern domain_finder = Pattern.compile("([a-z0-9][a-z0-9\\-]+\\.)*[a-z0-9][a-z0-9\\-]+\\.[a-z]{2,10}", Pattern.CASE_INSENSITIVE);
		private Text result_key = new Text();

		private String getDomainName(String uri) {
			Matcher domain_matches = domain_finder.matcher(uri);
			if (domain_matches.find()) {
				return domain_matches.group(0);
			}
			return null;
		}
		
		private final static Pattern pattern = Pattern.compile("content-type:\\s*(.*)", Pattern.CASE_INSENSITIVE);

		@Override
		public void map(Writable key, ClueWarcRecord doc, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			if (doc.getHeaderRecordType().equals("response")) {

				String request_uri = doc.getHeaderMetadataItem("WARC-Target-URI");
				String domain_name = getDomainName(request_uri);
				String content_type;
				if (domain_name != null) {
					
					byte[] byteContent = doc.getByteContent();
					String content = new String(byteContent);
					Matcher m = pattern.matcher(content);
					if (m.find()) {
						content_type = m.group(1);
						
					}
					else {
						content_type = "N/A";
					}
					
					result_key.set(domain_name+"	"+content_type);
					output.collect(result_key, one);
				}
			}
		}
	}

	public static class DomainPageContentTypeCounterReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		conf.setJobName("DomainPageCounter");

		// This line specifies the jar Hadoop should use to run the mapper and
		// reducer by telling it a class that’s inside it
		conf.setJarByClass(DomainPageContentTypeCounter.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(DomainPageContentTypeCounter.DomainPageContentTypeCounterMapper.class);
		conf.setReducerClass(DomainPageContentTypeCounter.DomainPageContentTypeCounterReducer.class);

		// KeyValueTextInputFormat treats each line as an input record,
		// and splits the line by the tab character to separate it into key and value
		conf.setInputFormat(ClueWarcInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);


		// Run this job locally
		conf.set("mapreduce.jobtracker.address", "local");
		conf.set("fs.defaultFS", "file:///");
		conf.set("cluewarcinputformat.skipsize", "4000000");

		String output_dir = "/home/martins/hadoop/output/";
		deleteLocalDir(new File(output_dir));

		FileInputFormat.setInputPaths(conf, new Path("/home/martins/hadoop/data/warcs/acis.lv/"));
		FileOutputFormat.setOutputPath(conf, new Path(output_dir));

		JobClient.runJob(conf);
	}

	private static boolean deleteLocalDir(File directory) {
		if (directory.exists()) {
			File[] files = directory.listFiles();
			if (null != files) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						deleteLocalDir(files[i]);
					} else {
						files[i].delete();
					}
				}
			}
		}
		return (directory.delete());
	}
}
