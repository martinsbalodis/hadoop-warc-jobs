package lv.edu.linux.hadoop.warc.jobs;

import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import java.util.ArrayList;
import java.util.List;
import lv.edu.linux.hadoop.warc.jobs.DomainFinder.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class DomainFinderTest {

	private MapDriver<Writable, ClueWarcRecord, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() {
		DomainFinderMapper mapper = new DomainFinderMapper();
		DomainFinderReducer reducer = new DomainFinderReducer();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testSingleDomainInRequestMapper() {
		ClueWarcRecord rec = new ClueWarcRecord();
		rec.setWarcRecordType("response");
		rec.setHeaderMetadataItem("WARC-Target-URI", "http://domain.lv");
		rec.setContent("");

		mapDriver.withInput(new Text("key"), rec);
		mapDriver.withOutput(new Text("domain.lv"), new Text("domain.lv"));
		mapDriver.runTest();
	}

	public void testSingleSubdomainInRequestMapper() {
		ClueWarcRecord rec = new ClueWarcRecord();
		rec.setWarcRecordType("response");
		rec.setHeaderMetadataItem("WARC-Target-URI", "http://subdomain.domain.lv");
		rec.setContent("");

		mapDriver.withInput(new Text("key"), rec);
		mapDriver.withOutput(new Text("domain.lv"), new Text("subdomain.domain.lv"));
		mapDriver.runTest();
	}

	public void testMultipleDomainsInContentMapper() {
		ClueWarcRecord rec = new ClueWarcRecord();
		rec.setWarcRecordType("response");
		rec.setHeaderMetadataItem("WARC-Target-URI", "http://domain.lv");
		rec.setContent("test1.domain.lv test2.domain.lv example.lv");

		mapDriver.withInput(new Text("key"), rec);
		mapDriver.withOutput(new Text("domain.lv"), new Text("test1.domain.lv"));
		mapDriver.withOutput(new Text("domain.lv"), new Text("test2.domain.lv"));
		mapDriver.withOutput(new Text("example.lv"), new Text("example.lv"));
		mapDriver.runTest();
	}

	@Test
	public void testNoSubdomainsReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("domain.lv"));
		values.add(new Text("domain.lv"));
		reduceDriver.withInput(new Text("domain.lv"), values);
		reduceDriver.withOutput(new Text("domain.lv"), new IntWritable(0));
		reduceDriver.runTest();
	}

	@Test
	public void testMultipleSubdomainsReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("test1.domain.lv"));
		values.add(new Text("test2.domain.lv"));
		reduceDriver.withInput(new Text("domain.lv"), values);
		reduceDriver.withOutput(new Text("domain.lv"), new IntWritable(2));
		reduceDriver.runTest();
	}
}
