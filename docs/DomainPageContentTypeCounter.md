# Domain Page Counter

This job will count page content types for each domain name. The output of this MapReduce job could be easily imported into HIVE for further analysis.

* Job class: `lv.edu.linux.hadoop.warc.jobs.DomainPageContentTypeCounter`

Output example:
```
1-veikals.lv	text/html; charset=iso-8859-1	1
100-atputa.viesumajas.lv	text/html	198
1000riepas.lv	text/html; charset=utf-8	2
100latviesustasti.lv	N/A	1
100latviesustasti.lv	text/plain	1
```


## Oozie specific settings

* `mapred.output.dir` : `/user/martins/output/${outputFolder}/`
* `mapred.input.dir` : `/dati/timekla-arhivs/${inputFolder}/`
* `mapred.mapoutput.key.class` : `org.apache.hadoop.io.Text`
* `mapred.mapoutput.value.class` : `org.apache.hadoop.io.IntWritable`
* `mapred.output.key.class` : `org.apache.hadoop.io.Text`
* `mapred.output.value.class` : `org.apache.hadoop.io.IntWritable`
* `mapred.mapper.class` : `lv.edu.linux.hadoop.warc.jobs.DomainPageContentTypeCounter$DomainPageContentTypeCounterMapper`
* `mapred.reducer.class` : `lv.edu.linux.hadoop.warc.jobs.DomainPageContentTypeCounter$DomainPageContentTypeCounterReducer`
* `mapred.input.format.class` : `edu.umd.cloud9.collection.clue.ClueWarcInputFormat`
* `mapred.output.format.class` : `org.apache.hadoop.mapred.TextOutputFormat`
* `cluewarcinputformat.skipsize` : `4000000`

