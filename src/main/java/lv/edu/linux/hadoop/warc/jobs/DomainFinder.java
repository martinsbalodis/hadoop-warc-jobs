package lv.edu.linux.hadoop.warc.jobs;

import edu.umd.cloud9.collection.clue.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class DomainFinder {

	public static class DomainFinderMapper extends MapReduceBase implements Mapper<Writable, ClueWarcRecord, Text, Text> {

		private Text output_top_domain = new Text();
		private Text output_sub_domain = new Text();
		private static Pattern domain_finder = Pattern.compile("((([a-z0-9\\-]+\\.)*)([a-z0-9\\-]+(\\.edu|\\.gov|\\.com|\\.id)?\\.lv))", Pattern.CASE_INSENSITIVE);
		
		private static List<String> found_domains = new ArrayList<String>();
		
		@Override
		public void map(Writable key, ClueWarcRecord doc, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			if (doc.getHeaderRecordType().equals("response")) {
				
				// self domain
				String request_uri = doc.getHeaderMetadataItem("WARC-Target-URI");
				Matcher request_uri_matches = domain_finder.matcher(request_uri);
				if (request_uri_matches.find()) {
					register_domain(request_uri_matches.group(1), request_uri_matches.group(4), request_uri_matches.group(2), output);
				}
				
				
				// @TODO doubles used memory :(
				byte[] byteContent = doc.getByteContent();
				String content = new String(byteContent);
				Matcher m = domain_finder.matcher(content);
				while (m.find()) {
					register_domain(m.group(1), m.group(4), m.group(2), output);
				}
			}
		}
		
		public void register_domain(String full_domain_name, String top_domain_name, String sub_domain, OutputCollector<Text, Text> output) throws IOException {
			
			full_domain_name = full_domain_name.toLowerCase();
			top_domain_name = top_domain_name.toLowerCase();
			sub_domain = sub_domain.toLowerCase();
			
			if(!found_domains.contains(full_domain_name)) {
				output_top_domain.set(top_domain_name);
				output_sub_domain.set(full_domain_name);
				output.collect(output_top_domain, output_sub_domain);
				found_domains.add(full_domain_name);
			}
		}
	}

	public static class DomainFinderReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
		
		private static List<String> found_sub_domains = new ArrayList<String>();
		
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int subdomain_count = 0;
			String domain_name = key.toString();
			while (values.hasNext()) {
				String sub_domain_name = values.next().toString();
				if(!found_sub_domains.contains(sub_domain_name)) {
					// Don't count top domains as subdomains
					if(!sub_domain_name.equals(domain_name)) {
						subdomain_count += 1;
						found_sub_domains.add(sub_domain_name);
					}
				}
			}
			output.collect(key, new IntWritable(subdomain_count));
		}
	}
	
		public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		conf.setJobName("DomainFinder");

		// This line specifies the jar Hadoop should use to run the mapper and
		// reducer by telling it a class that’s inside it
		conf.setJarByClass(DomainFinder.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(DomainFinderMapper.class);
		conf.setReducerClass(DomainFinderReducer.class);

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
