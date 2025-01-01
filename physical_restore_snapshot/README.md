mysql_upgrade_info > contains mariadb version


docker run --rm -it --add-host=host.docker.internal:host-gateway ubuntu
curl http://host.docker.internal

------

What we have:
- Local MariaDB
    - Running at :3306
    - Has data directory on /var/lib/mysql
    - Has it's root user password
- Volume holding backup
    - /mnt/backup1
- Backup database name
- Target database name
- Container
    - will know the password of root user
    - can access network namespace of host
    - have it's own network namespace
    - user is root also

Container will get what ?
- Volumes
    - /mnt/backup1:/var/lib/mysql > dummy mysql server
    - /var/lib/mysql/_database_directory:/db > older, where we need to put the ibd and cfg files
- UID and GID of mysql
- Backup Database Name
- Target Database Name

Assumptions -
- Non partitioned table
- Don't reuse the mounted backup volume

> Just start the container, it will do it's job and exit

Steps -
1. Change uid/gid of mysql user
2. Start the local mysql server in background (keep pid handy to give a graceful shutdown)
3. Wait for connection on localhost:3306 port
4. Use mysqldump to dump the schema of the database
5. Delete all tables of it
6. Ensure no cfg, ifb files there
7. Create all table views from dump
8. Run `FLUSH TABLES` on backup server
9. Copy the contents to /db
10. Release lock from backup server
11. `Import Tablespace` on real server
12. Done !



/target_db
/var/lib/mysql

env variables
BACKUP_DB=
TARGET_DB=

TARGET_DB_ROOT_PASSWORD
BACKUP_DB_ROOT_PASSWORD
TARGET_DB_PORT=3306
TARGET_DB_HOST=localhost