FROM mariadb:10.11.8

ADD --chown=root:root ./custom-mariadb.cnf /etc/mysql/mariadb.conf.d/custom-mariadb.cnf

# Rewrite the entrypoint
COPY docker-entrypoint.sh /usr/local/bin/