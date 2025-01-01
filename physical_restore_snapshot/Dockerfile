FROM mariadb:10.11.8

# Install python
RUN apt-get update && apt-get install -y python3 python3-pip python-is-python3

# Install required python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Copy the script to the container
COPY restore.py /restore.py

ADD --chown=root:root ./custom-mariadb.cnf /etc/mysql/mariadb.conf.d/custom-mariadb.cnf

# Rewrite the entrypoint
COPY docker-entrypoint.sh /usr/local/bin/