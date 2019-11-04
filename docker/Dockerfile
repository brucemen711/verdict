FROM ubuntu:18.04

RUN apt -y update
RUN apt install -y vim redis curl openjdk-11-jdk

# # Install Oracle Java10
# RUN add-apt-repository ppa:webupd8team/java
# RUN apt update
# RUN apt install oracle-java8-installer
# RUN apt-get install oracle-java10-set-default

RUN curl -s -o /root/Miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh 
RUN bash /root/Miniconda.sh -b -p /root/miniconda
ENV PATH="/root/miniconda/bin:${PATH}"

# Install necessary Python packages
RUN conda install -y pyhive[presto] pyhive[hive] jupyterlab pandas nodejs
RUN conda install -y -c anaconda psutil python-daemon
RUN conda install -y -c plotly plotly==4.1.1
RUN conda install jupyterlab=1.0 "ipywidgets=7.5"
RUN export NODE_OPTIONS=--max-old-space-size=4096
RUN jupyter labextension install @jupyter-widgets/jupyterlab-manager@1.0 --no-build
RUN jupyter labextension install plotlywidget@1.1.1 --no-build
RUN jupyter labextension install jupyterlab-plotly@1.1.2 --no-build
RUN jupyter labextension install jupyterlab-chart-editor@1.2 --no-build
RUN jupyter lab build
RUN unset NODE_OPTIONS

RUN mkdir /opt/notebooks
RUN curl -s -o /root/zeppelin-0.8.1-bin-all.tgz http://apache.cs.utah.edu/zeppelin/zeppelin-0.8.1/zeppelin-0.8.1-bin-all.tgz
RUN tar xzf /root/zeppelin-0.8.1-bin-all.tgz -C /root 
RUN chown -R root:root /root/zeppelin-0.8.1-bin-all		# weirdly, the owner is set to 503 after unzip

# Copy and install verdict package
RUN mkdir /root/verdict
COPY verdict /root/verdict/verdict
COPY LICENSE /root/verdict/LICENSE
COPY setup.py /root/verdict/setup.py
COPY README.md /root/verdict/README.md
RUN pip install /root/verdict

# Create a verdict dir and the location for cache
RUN mkdir -p /usr/local/var/verdict
RUN mkdir -p /usr/local/var/verdict/cache
RUN mkdir -p /usr/local/var/verdict/log

# Copy a bootstrap script
RUN mkdir /root/bootstrap
COPY docker/*.sh /root/bootstrap/
RUN chown root:root /root/bootstrap/*.sh
RUN chmod 700 /root/bootstrap/*.sh
COPY docker/*.py /root/bootstrap/

# Copy zeppelin notebook
RUN mkdir /root/zeppelin-0.8.1-bin-all/notebook/2EMM5HJDB /root/zeppelin-0.8.1-bin-all/notebook/2ESF1KHY9
COPY docker/zeppelin_notebook/2EMM5HJDB/note.json /root/zeppelin-0.8.1-bin-all/notebook/2EMM5HJDB/note.json
COPY docker/zeppelin_notebook/2ESF1KHY9/note.json /root/zeppelin-0.8.1-bin-all/notebook/2ESF1KHY9/note.json

# Copy Jupyter notebook
COPY docker/jupyter_notebook /opt/notebooks/

# Entry point
ENV PATH="/root/bootstrap:${PATH}"

CMD ["/root/bootstrap/start_services.sh"]

# ports for jupyter and zeppelin notebook server
EXPOSE 8888 8180
