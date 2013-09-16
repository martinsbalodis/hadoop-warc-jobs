package lv.edu.linux.hadoop.warc.jobs;

import edu.umd.cloud9.collection.clue.ClueWarcInputFormat;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class SsLvVehicleExtract {
		public static class SsLvVehicleExtractMapper extends MapReduceBase implements Mapper<Writable, ClueWarcRecord, NullWritable, Text> {

		private Text output_data = new Text();
		
		private final static Pattern car_listing_finder = Pattern.compile("http://www.ss.lv/lv/transport/cars/([a-z0-9\\-]+)/([a-z0-9\\-]+)/(page[0-9]+\\.html)?$", Pattern.CASE_INSENSITIVE);
		private final static NullWritable key_null = NullWritable.get();
		
		private final static NumberFormat price_number_format = NumberFormat.getNumberInstance(Locale.UK);
		
		@Override
		public void map(Writable key, ClueWarcRecord doc, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

			if (doc.getHeaderRecordType().equals("response")) {
				
				// self domain
				String request_uri = doc.getHeaderMetadataItem("WARC-Target-URI");
				Matcher request_uri_matches = car_listing_finder.matcher(request_uri);
				if (request_uri_matches.find()) {
					
					byte[] byteContent = doc.getByteContent();
					String content = new String(byteContent);
					
					Document html_doc = Jsoup.parse(content);
					Elements rows = html_doc.select("table:nth-of-type(3) tr:nth-of-type(n+2)");
					for(int i = 0;i<rows.size();i++) {
						try {
							Element row = rows.get(i);
							
							String price_str = row.select("td.msga2-o:nth-of-type(7)").get(0).text().trim();
							if(price_str == "pērku") {
								continue;
							}
							
							String title = row.select("td.msg2").get(0).text().replaceAll(",",";");
							int year = Integer.parseInt(row.select("td.msga2-o:nth-of-type(4)").get(0).text().replaceAll("[^0-9]+",""));
							String engine = row.select("td.msga2-o:nth-of-type(5)").get(0).text();
							int mileage = Integer.parseInt(row.select("td.msga2-r").get(0).text().replaceAll("[^0-9]+",""));
							int price = Integer.parseInt(price_str.replaceAll("[^0-9]+",""));
							
							String engine_type = "petrol";
							if(engine.endsWith("D")) {
								engine_type = "diesel";
							}
							float engine_capacity = Float.parseFloat(engine.replaceAll("[^0-9\\.]+",""));
							
							String make = request_uri_matches.group(1);
							String model = request_uri_matches.group(2);
							
							String result = title+','+make+','+model+','
									+year+','+engine_capacity+','
									+engine_type+','+mileage+','+price;
							
							output_data.set(result);
							output.collect(key_null, output_data);
						}
						catch (NullPointerException e) {}
						catch (NumberFormatException e) {}
						catch (IndexOutOfBoundsException e){
							System.err.println(e.getMessage());
						}
					}
				}
			}
		}
	}
		
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		conf.setJobName("SsLvCarListingExtractor");

		// This line specifies the jar Hadoop should use to run the mapper and
		// reducer by telling it a class that’s inside it
		conf.setJarByClass(SsLvVehicleExtract.class);

		conf.setMapOutputKeyClass(NullWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(SsLvVehicleExtract.SsLvVehicleExtractMapper.class);

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

		FileInputFormat.setInputPaths(conf, new Path("/home/martins/hadoop/data/warcs/ss.lv/"));
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
