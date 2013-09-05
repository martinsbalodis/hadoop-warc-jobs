# Latvian domain name extractor

This job will extract all domain names from archives and also counts subdomains for each domain.

* Job class: `lv.edu.linux.hadoop.warc.jobs.DomainFinder`
* Test class `lv.edu.linux.hadoop.warc.jobs.DomainFinderTest`

Output example:
```
acis.lv 1
acuarsts.lv     1
acuklinika.lv   1
bernukardiologija.lv    1
lu.lv   2
skrickis.lv     1
stradini.lv     1
urbanpicture.lv 1
```


## Oozie specific settings

* `mapred.output.dir` : `/user/martins/output/${outputFolder}/`
* `mapred.input.dir` : `/dati/timekla-arhivs/latvijas-pasvaldibas/`
* `mapred.mapoutput.key.class` : `org.apache.hadoop.io.Text`
* `mapred.mapoutput.value.class` : `org.apache.hadoop.io.Text`
* `mapred.output.key.class` : `org.apache.hadoop.io.Text`
* `mapred.output.value.class` : `org.apache.hadoop.io.IntWritable`
* `mapred.mapper.class` : `lv.edu.linux.hadoop.warc.jobs.DomainFinder$DomainFinderMapper`
* `mapred.reducer.class` : `lv.edu.linux.hadoop.warc.jobs.DomainFinder$DomainFinderReducer`
* `mapred.input.format.class` : `edu.umd.cloud9.collection.clue.ClueWarcInputFormat`
* `mapred.output.format.class` : `org.apache.hadoop.mapred.TextOutputFormat`
* `cluewarcinputformat.skipsize` : `4000000`

