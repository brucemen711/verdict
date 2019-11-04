#!/bin/bash

# start redis server
redis-server --daemonize yes

# start pandas sql server
nohup pandas-sql-server --preload-cache &

# This starts Zeppelin
/root/zeppelin-0.8.1-bin-all/bin/zeppelin-daemon.sh start

# This starts Jupyter notebook
jupyter notebook --notebook-dir /opt/notebooks \
    --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token=''
