# A list of tools which work with warc archives.


## Latvian domain name extractor

This job will extract all domain names from archives and also counts subdomains for each domain. Detailed info [here][1]

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

## Domain Page Counter

This job will count page content types for each domain name. Detailed info [here][3]

Output example:
```
1-veikals.lv    text/html; charset=iso-8859-1   1
100-atputa.viesumajas.lv        text/html       198
1000riepas.lv   text/html; charset=utf-8        2
100latviesustasti.lv    N/A     1
100latviesustasti.lv    text/plain      1
```


  [1]: docs/DomainNameExtractor.md
  [2]: docs/DomainPageCounter.md
  [3]: docs/DomainPageContentTypeCounter.md


