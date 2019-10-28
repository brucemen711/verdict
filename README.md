<!-- [![Build Status](https://circleci.com/gh/mozafari/verdictdb/tree/master.svg?style=shield&circle-token=16a7386340ff7022b21ce007434f8caa2fa97aec)](https://circleci.com/gh/mozafari/verdictdb/tree/master) -->
<!-- [![CircleCI branch](https://img.shields.io/circleci/project/github/mozafari/verdictdb/master.svg)](https://circleci.com/gh/mozafari/verdictdb/tree/master) -->

**We are making lots of changes right now.**

Project website: https://verdictdb.org

Documentation: https://verdict.readthedocs.org


<!-- [![Build Status](https://circleci.com/gh/mozafari/verdictdb/tree/master.svg?style=shield&circle-token=16a7386340ff7022b21ce007434f8caa2fa97aec)](https://circleci.com/gh/mozafari/verdictdb/tree/master)
[![Code Coverage](https://codecov.io/gh/mozafari/verdictdb/branch/master/graph/badge.svg)](https://codecov.io/gh/mozafari/verdictdb)
[![JDK](https://img.shields.io/badge/JDK-7,%208-green.svg)]()
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.maven/apache-maven.svg)](https://search.maven.org/artifact/org.verdictdb/verdictdb-core) -->
[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/verdictdb/chat)



# Instant analytics, however big your data is

<p align="center">
<img src="http://verdictdb.org/image/verdict-for-impala-speedup.png" width="600px" />
</p>

Verdict brings interactive-speed, resource-efficient data analytics.

1. **200x faster by sacrificing only 1% accuracy**
   Verdict can give you 99% accurate answers for your big data queries in a fraction of the time 
   needed for calculating exact answers. If your data is too big to analyze in a couple of seconds, 
   you will like Verdict.
2. **No change to your database**
   Verdict is a middleware standing between your application and your backend engine. You can just 
   issue the same queries as before and get precise estimates computed instantly.
3. **Runs on (almost) any database**
   Verdict is designed to run on any database that supports standard SQL. Right now, we support
   Presto and will soon add other open source engines.
4. **Ease of use**
   Verdict is a light-weight client-side library: no servers, no port configurations, no extra user 
   authentication, etc., beyond what you already have.

Find out more about Verdict by visiting [verdict.org](https://verdictdb.org) and 
the [documentation](https://verdict.readthedocs.org).


## Quickstart

Start Verdict in a single line with `docker-compose` (with Presto for its backend engine).

```bash
curl -s https://raw.githubusercontent.com/verdictproject/verdict/master/docker-compose-64gb.yaml \
    | docker-compose -f - up
```

Once the docker containers run, start the Python shell.

```bash
docker exec -it docker-verdict python
```

```python
import verdict
v = verdict.presto(presto_host='presto')
```


### Queries run slower originally

```python
v.sql('bypass select count(*) from tpch.sf1.orders')
# Returning an answer in 8.863600015640259 sec(s). 
#      _col0
# 0  1500000

v.sql('''\
   bypass select orderpriority, count(*) 
   from tpch.sf1.lineitem 
   group by orderpriority 
   order by orderpriority''')
# Returning an answer in 9.716013669967651 sec(s). 
#     _col0
# 0  300343
# 1  300091
# 2  298723
# 3  300254
# 4  300589
```


### Create a sample (one time)

```python
v.create_sample('tpch.sf1.orders')
```


### Now queries run fast

```python
v.sql('select count(*) from tpch.sf1.orders')
# Returning an answer in 0.17403197288513184 sec(s). 
#         c1
# 0  1503884

v.sql('''\
   select orderpriority, count(*) 
   from tpch.sf1.orders 
   group by orderpriority 
   order by orderpriority''')
# Returning an answer in 0.14169764518737793 sec(s). 
#      orderpriority      c1
# 0         1-URGENT  300784
# 1           2-HIGH  301540
# 2         3-MEDIUM  298872
# 3  4-NOT SPECIFIED  302060
# 4            5-LOW  300628
```

These comparisons above are more to help you quickly see the potential performance gains (than
being scientific).

For more large-scale (controlled) examples, see our 
[quickstart guide](https://verdict.readthedocs.io/en/latest/quickstart.html).


## How it works

<p align="center">
<img src="http://verdictdb.org/image/verdict-architecture.png" width="500px" />
</p>

1. Verdict rewrites your queries to use special types of *samples* (instead of original tables).
2. The rewritten queries are processed by the backend engine.
3. Given the answers from the engine, Verdict compose statistically unbiased estimates 
   (for your final answers), which are returned.

Even the samples are stored in your engines/stores (database, S3, and so on). Thus, almost no
privacy issues.




## More information

- Research: https://verdictdb.org/documentation/research/


<!-- ## Free for all

We maintain VerdictDB for free under the Apache License so that anyone can benefit from these contributions. If you like our project, please star our Github repository (https://github.com/mozafari/verdictdb) and send your feedback to verdict-user@umich.edu. -->

