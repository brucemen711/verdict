#!/bin/bash

# start redis server
redis-server --daemonize yes

# start verdict server
#python /root/bootstrap/wait_for_presto.py
#nohup verdict-server --cache-server-host presto start &

# This starts Zeppelin
/root/zeppelin-0.8.1-bin-all/bin/zeppelin-daemon.sh start

# This starts Jupyter notebook
jupyter lab --notebook-dir /opt/notebooks \
    --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token=''
