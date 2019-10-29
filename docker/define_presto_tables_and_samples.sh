#!/bin/bash

python /root/bootstrap/wait_for_presto.py
python /root/bootstrap/define_tpch_tables.py
python /root/bootstrap/define_tpch_samples.py
python /root/bootstrap/define_instacart_tables.py
python /root/bootstrap/define_instacart_samples.py

# download meta
redis-cli shutdown
pip install awscli
aws s3 cp s3://verdictpublic/verdict_meta/0.1.6/dump.rdb /dump.rdb
pushd /
redis-server --daemonize yes
popd

# download cache
pandas-sql-server stop
aws s3 cp --recursive s3://verdictpublic/verdict_cache/0.1.6/ /usr/local/var/verdict/cache/
nohup pandas-sql-server --preload-cache &

#echo "waiting for 10 secs for redis to load data from the dump"
#sleep 10
#verdict-server stop
#nohup verdict-server --cache-server-host presto start &

echo 'DONE!'
