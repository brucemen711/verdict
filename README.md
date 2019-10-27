# Verdict


## Quickstart


### Install

```
docker run verdictproject/verdict
```


### Create Samples


### Run Queries

In Python 3.5 or above,

```python
import verdict

v = verdict.presto()
v.sql("select count(*) from lineitem")
```

See documentation for more examples and core concepts.


## Documentation

See: https://verdict.readthedocs.io/en/latest/