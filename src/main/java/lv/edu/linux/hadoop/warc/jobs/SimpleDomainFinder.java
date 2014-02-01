package lv.edu.linux.hadoop.warc.jobs;

import edu.umd.cloud9.collection.clue.*;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SimpleDomainFinder {

	public static class SimpleDomainFinderMapper extends MapReduceBase implements Mapper<Writable, ClueWarcRecord, Text, NullWritable> {

		private Text output_domain = new Text();
		private static Pattern domain_finder = Pattern.compile("[a-z0-9][a-z0-9-\\.]+\\.lv", Pattern.CASE_INSENSITIVE);
		private static final NullWritable output_null = NullWritable.get();
		
		@Override
		public void map(Writable key, ClueWarcRecord doc, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {

			if (doc.getHeaderRecordType().equals("response")) {
				
				// self domain
				String request_uri = doc.getHeaderMetadataItem("WARC-Target-URI");
				Matcher request_uri_matches = domain_finder.matcher(request_uri);
				if (request_uri_matches.find()) {
					String domain = request_uri_matches.group(0).toLowerCase();
					output_domain.set(domain);
					output.collect(output_domain, output_null);
				}
				
				
				// @TODO doubles used memory :(
				byte[] byteContent = doc.getByteContent();
				String content = new String(byteContent);
				Matcher m = domain_finder.matcher(content);
				while (m.find()) {
					String domain = m.group(0).toLowerCase();
					output_domain.set(domain);
					output.collect(output_domain, output_null);
				}
			}
		}
	}

	public static class SimpleDomainFinderReducer extends MapReduceBase implements Reducer<Text, NullWritable, Text, NullWritable> {
		
		private static final NullWritable output_null = NullWritable.get();
		
		@Override
		public void reduce(Text key, Iterator<NullWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
			output.collect(key, output_null);
		}
	}
	
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		conf.setJobName("SimpleDomainFinder");

		// This line specifies the jar Hadoop should use to run the mapper and
		// reducer by telling it a class that’s inside it
		conf.setJarByClass(SimpleDomainFinder.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(NullWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(SimpleDomainFinderMapper.class);
		conf.setReducerClass(SimpleDomainFinderReducer.class);

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
