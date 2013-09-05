# Domain Page Counter

This job will count pages for each domain name within archives.

* Job class: `lv.edu.linux.hadoop.warc.jobs.DomainPageCounter`

Output example:
```
acis.lv 1
acuarsts.lv     12354
acuklinika.lv   145
bernukardiologija.lv    154
lu.lv   243553
skrickis.lv     111
stradini.lv     123
urbanpicture.lv 134
```


## Oozie specific settings

* `mapred.output.dir` : `/user/martins/output/${outputFolder}/`
* `mapred.input.dir` : `/dati/timekla-arhivs/${inputFolder}/`
* `mapred.mapoutput.key.class` : `org.apache.hadoop.io.Text`
* `mapred.mapoutput.value.class` : `org.apache.hadoop.io.IntWritable`
* `mapred.output.key.class` : `org.apache.hadoop.io.Text`
* `mapred.output.value.class` : `org.apache.hadoop.io.IntWritable`
* `mapred.mapper.class` : `lv.edu.linux.hadoop.warc.jobs.DomainPageCounter$DomainPageCounterMapper`
* `mapred.reducer.class` : `lv.edu.linux.hadoop.warc.jobs.DomainPageCounter$DomainPageCounterReducer`
* `mapred.input.format.class` : `edu.umd.cloud9.collection.clue.ClueWarcInputFormat`
* `mapred.output.format.class` : `org.apache.hadoop.mapred.TextOutputFormat`
* `cluewarcinputformat.skipsize` : `4000000`

