# A list of tools which work with warc archives.


## Latvian domain name extractor

This job will extract all domain names from archives and also counts subdomains for each domain. Detailed info [here][1]

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

## Domain page counter

This job will count pages found per domain name. Detailed info [here][2]

  [1]: docs/DomainNameExtractor.md
  [2]: docs/DomainPageCounter.md

