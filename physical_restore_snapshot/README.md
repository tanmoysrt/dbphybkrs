**Actual Steps**

1. Create a dummy database on target server `create database abc_demo`
2. Run the container

```bash
   sudo docker run --rm -it \
    --add-host=host.docker.internal:host-gateway \
    -v /home/tanmoy/Desktop/physical-backup/phy2:/var/lib/mysql \
    -v /var/lib/mysql/abc_demo/:/target_db \
    -e BACKUP_DB="employees" \
    -e TARGET_DB="abc_demo" \
    -e TARGET_DB_ROOT_PASSWORD="toor" \
    -e BACKUP_DB_ROOT_PASSWORD="toor" \
    -e TARGET_DB_HOST="host.docker.internal" \
    -e TARGET_DB_PORT=3306 \
    -e MYSQL_UID=127 \
    -e MYSQL_GID=135 \
    db_restore
```

**What we have:**

- Local MariaDB
  - Running at :3306
  - Has data directory on /var/lib/mysql
  - Has it's root user password
- Volume holding backup
  - /mnt/backup1
- Backup database name
- Target database name

**Container will get what ?**

- Volumes
  - /mnt/backup1:/var/lib/mysql > dummy mysql server
  - /var/lib/mysql/\_database_directory:/db > older, where we need to put the ibd and cfg files
- UID and GID of mysql
- Backup Database Name
- Target Database Name

**Assumptions -**

- Non partitioned table
- Don't reuse the mounted backup volume

> Just start the container, it will do it's job and exit

**Steps -**

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

---

For local testing, grant root access from any host -
```sql
ALTER USER 'root'@'localhost' IDENTIFIED BY 'toor';
ALTER USER 'root'@'%' IDENTIFIED BY 'toor';
ALTER USER 'root'@'::1' IDENTIFIED BY 'toor';
ALTER USER 'root'@'127.0.0.1' IDENTIFIED BY 'toor';
GRANT ALL PRIVILEGES ON _._ TO 'root'@'localhost' IDENTIFIED BY 'toor' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON _._ TO 'root'@'%' IDENTIFIED BY 'toor' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON _._ TO 'root'@'::1' IDENTIFIED BY 'toor' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON _._ TO 'root'@'127.0.0.1' IDENTIFIED BY 'toor' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```
