#!/bin/bash

docker cp docker-verdict:/opt/notebooks/NoVerdict.ipynb jupyter_notebook/
docker cp docker-verdict:/opt/notebooks/WithVerdict.ipynb jupyter_notebook/
mkdir -p zeppelin_notebook/2EMM5HJDB zeppelin_notebook/2ESF1KHY9
docker cp docker-verdict:/root/zeppelin-0.8.1-bin-all/notebook/2EMM5HJDB/note.json zeppelin_notebook/2EMM5HJDB/note.json
docker cp docker-verdict:/root/zeppelin-0.8.1-bin-all/notebook/2ESF1KHY9/note.json zeppelin_notebook/2ESF1KHY9/note.json
