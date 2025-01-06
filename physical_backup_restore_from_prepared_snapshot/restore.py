import shutil
import peewee
import os
import re
import subprocess


class ConnectionClosedWithDatabase(Exception):
    pass


class DatabaseSchemaExportError(Exception):
    pass


class DatabaseImporter:
    def __init__(
        self,
        backup_db: str,
        target_db: str,
        target_db_root_password: str,
        target_db_port: int,
        target_db_host: str,
        innodb_tables: list[str],
        myisam_tables: list[str],
        table_schema: list[str],
        backup_db_base_directory: str,  # /mnt/tmp1/var/lib/mysql
        target_db_base_directory: str = "/var/lib/mysql",
    ):
        self._target_db_instance: peewee.MySQLDatabase = None
        self._target_db_instance_for_myisam: peewee.MySQLDatabase = None
        self.target_db = target_db
        self.target_db_user = "root"
        self.target_db_password = target_db_root_password
        self.target_db_host = target_db_host
        self.target_db_port = target_db_port
        self.target_db_directory = os.path.join(target_db_base_directory, target_db)

        self.backup_db = backup_db
        self.backup_db_directory = os.path.join(backup_db_base_directory, backup_db)

        self.innodb_tables = innodb_tables
        self.myisam_tables = myisam_tables
        self.table_schema = table_schema

    def process(self):
        self.get_target_db()
        print("Validated connection with target db")
        print("-- Starting the process --")
        self._check_and_fix_myisam_table_files()
        print("Checked and fixed MyISAM table files")
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#importing-transportable-tablespaces-for-non-partitioned-tables
        self._prepare_target_db_for_restore()
        print("Prepared target db")
        self._create_tables_from_backup()
        print("Created tables from backup")
        self._discard_tablespaces_from_target_db()
        print("Discarded tablespaces from target db")
        self._perform_file_operations(engine="innodb")
        print("Performed file operations related to InnoDB")
        self._import_tablespaces_in_target_db()
        print("Imported tablespaces in target db")
        self._take_write_lock_on_myisam_tables()
        print("Took write lock on tables")
        self._perform_file_operations(engine="myisam")
        print("Performed file operations related to MyISAM")
        self.unlock_all_tables()
        print("Unlocked all tables")
        print("-- Process completed --")

    def _prepare_target_db_for_restore(self):
        """
        Prepare the database for import
        - fetch existing tables list in database
        - delete all tables
        """
        tables = self.get_target_db().get_tables()
        # before start dropping tables, disable foreign key checks
        # it will reduce the time to drop tables and will not cause any block while dropping tables
        self.get_target_db().execute_sql("SET SESSION FOREIGN_KEY_CHECKS = 0;")
        for table in tables:
            self.get_target_db().execute_sql("DROP TABLE IF EXISTS {};".format(table))
        self.get_target_db().execute_sql(
            "SET SESSION FOREIGN_KEY_CHECKS = 1;"
        )  # re-enable foreign key checks

    def _create_tables_from_backup(self):
        # https://github.com/frappe/frappe/pull/26855
        schema_file_content: str = re.sub(
            r"/\*M{0,1}!999999\\- enable the sandbox mode \*/",
            "",
            self.table_schema,
        )
        # # https://github.com/frappe/frappe/pull/28879
        schema_file_content: str = re.sub(
            r"/\*![0-9]* DEFINER=[^ ]* SQL SECURITY DEFINER \*/",
            "",
            self.table_schema,
        )
        # create the tables
        sql_stmts = schema_file_content.split(";\n")
        for sql_stmt in sql_stmts:
            if sql_stmt.strip():
                self.get_target_db().execute_sql(sql_stmt)

    def _discard_tablespaces_from_target_db(self):
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#foreign-key-constraints
        self.get_target_db().execute_sql("SET SESSION foreign_key_checks = 0;")
        for table in self.innodb_tables:
            self.get_target_db().execute_sql(
                "ALTER TABLE {} DISCARD TABLESPACE;".format(table)
            )
        self.get_target_db().execute_sql(
            "SET SESSION foreign_key_checks = 1;"
        )  # re-enable foreign key checks

    def _take_write_lock_on_myisam_tables(self):
        """
        MyISAM doesn't support foreign key constraints
        So, need to take write lock on MyISAM tables

        Discard tablespace query on innodb already took care of locks
        """
        tables = ["`{}` WRITE".format(table) for table in self.myisam_tables]
        self.get_target_db_for_myisam().execute_sql(
            "LOCK TABLES {};".format(", ".join(tables))
        )

    def _perform_file_operations(self, engine: str):
        for file in os.listdir(self.backup_db_directory):
            # copy only .ibd, .cfg if innodb
            if engine == "innodb" and not (
                file.endswith(".ibd") or file.endswith(".cfg")
            ):
                continue

            # copy one .MYI, .MYD files if myisam
            if engine == "myisam" and not (
                file.endswith(".MYI") or file.endswith(".MYD")
            ):
                continue

            shutil.copy(
                os.path.join(self.backup_db_directory, file),
                os.path.join(self.target_db_directory, file),
            )

        # change ownership to mysql user and group and mode to 660
        for file in os.listdir(self.target_db_directory):
            file_path = os.path.join(self.target_db_directory, file)
            os.chmod(file_path, 0o660)
            shutil.chown(file_path, user="mysql", group="mysql")

    def _import_tablespaces_in_target_db(self):
        for table in self.innodb_tables:
            self.get_target_db().execute_sql(
                "ALTER TABLE {} IMPORT TABLESPACE;".format(table)
            )

    def _check_and_fix_myisam_table_files(self):
        """
        Check issues in MyISAM table files
        myisamchk :path

        If any issues found, try to repair the table
        """
        files = os.listdir(self.target_db_directory)
        files = [file for file in files if file.endswith(".MYI")]
        for file in files:
            myisamchk_command = [
                "myisamchk",
                os.path.join(self.target_db_directory, file),
            ]
            try:
                subprocess.check_output(myisamchk_command)
                print("Checked MyISAM table file: {}".format(file))
            except subprocess.CalledProcessError as e:
                print("Error while checking MyISAM table file: {}".format(e.output))
                print("Trying to repair the table")
                myisamchk_command.append("--recover")
                try:
                    subprocess.check_output(myisamchk_command)
                except subprocess.CalledProcessError as e:
                    print(
                        "Error while repairing MyISAM table file: {}".format(e.output)
                    )
                    print("Stopping the process")
                    raise Exception from e

    # private methods
    def get_target_db(self) -> peewee.MySQLDatabase:
        if self._target_db_instance is not None:
            if not self._target_db_instance.is_connection_usable():
                raise ConnectionClosedWithDatabase()
            return self._target_db_instance

        self._target_db_instance = peewee.MySQLDatabase(
            self.target_db,
            user=self.target_db_user,
            password=self.target_db_password,
            host=self.target_db_host,
            port=self.target_db_port,
        )
        self._target_db_instance.connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._target_db_instance.execute_sql("SET SESSION wait_timeout = 14400;")
        return self._target_db_instance

    def get_target_db_for_myisam(self) -> peewee.MySQLDatabase:
        if self._target_db_instance_for_myisam is not None:
            if not self._target_db_instance_for_myisam.is_connection_usable():
                raise ConnectionClosedWithDatabase()
            return self._target_db_instance_for_myisam

        self._target_db_instance_for_myisam = peewee.MySQLDatabase(
            self.target_db,
            user=self.target_db_user,
            password=self.target_db_password,
            host=self.target_db_host,
            port=self.target_db_port,
            autocommit=False,
        )
        self._target_db_instance_for_myisam.connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._target_db_instance_for_myisam.execute_sql(
            "SET SESSION wait_timeout = 14400;"
        )
        return self._target_db_instance_for_myisam

    def unlock_all_tables(self):
        self.get_target_db().execute_sql("UNLOCK TABLES;")
        self.get_target_db_for_myisam().execute_sql("UNLOCK TABLES;")

    def __del__(self):
        if self._target_db_instance is not None:
            self._target_db_instance.close()
        if self._target_db_instance_for_myisam is not None:
            self._target_db_instance_for_myisam.close()


if __name__ == "__main__":
    importer = DatabaseImporter(
        backup_db="askubuntu",
        target_db="def_demo",
        target_db_root_password="toor",
        target_db_port=3306,
        target_db_host="localhost",
        innodb_tables=[
            "badges",
            "comments",
            "posts",
            "post_history",
            "post_links",
            "tags",
            "votes",
        ],
        myisam_tables=["users"],
        table_schema="/*!999999\\- enable the sandbox mode */ \n-- MariaDB dump 10.19  Distrib 10.11.8-MariaDB, for debian-linux-gnu (x86_64)\n--\n-- Host: localhost    Database: askubuntu\n-- ------------------------------------------------------\n-- Server version\t10.11.8-MariaDB-0ubuntu0.24.04.1\n\n/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n/*!40101 SET NAMES utf8mb4 */;\n/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;\n/*!40103 SET TIME_ZONE='+00:00' */;\n/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n\n--\n-- Table structure for table `badges`\n--\n\nDROP TABLE IF EXISTS `badges`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `badges` (\n  `id` int(11) DEFAULT NULL,\n  `user_id` int(11) DEFAULT NULL,\n  `badge_name` varchar(500) DEFAULT NULL,\n  `badge_date` datetime DEFAULT NULL,\n  `class` int(11) DEFAULT NULL,\n  `tag_based` varchar(10) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n\n--\n-- Table structure for table `comments`\n--\n\nDROP TABLE IF EXISTS `comments`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `comments` (\n  `id` int(11) DEFAULT NULL,\n  `post_id` int(11) DEFAULT NULL,\n  `score` int(11) DEFAULT NULL,\n  `comment_text` varchar(4000) DEFAULT NULL,\n  `creation_date` datetime DEFAULT NULL,\n  `user_id` int(11) DEFAULT NULL,\n  `content_license` varchar(100) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n\n--\n-- Table structure for table `post_history`\n--\n\nDROP TABLE IF EXISTS `post_history`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `post_history` (\n  `id` int(11) DEFAULT NULL,\n  `post_history_type_id` int(11) DEFAULT NULL,\n  `post_id` int(11) DEFAULT NULL,\n  `revision_guid` varchar(100) DEFAULT NULL,\n  `creation_date` datetime DEFAULT NULL,\n  `user_id` int(11) DEFAULT NULL,\n  `post_text` varchar(10000) DEFAULT NULL,\n  `content_license` varchar(100) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n\n--\n-- Table structure for table `post_links`\n--\n\nDROP TABLE IF EXISTS `post_links`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `post_links` (\n  `id` int(11) DEFAULT NULL,\n  `creation_date` datetime DEFAULT NULL,\n  `post_id` int(11) DEFAULT NULL,\n  `related_post_id` int(11) DEFAULT NULL,\n  `link_type_id` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n\n--\n-- Table structure for table `posts`\n--\n\nDROP TABLE IF EXISTS `posts`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `posts` (\n  `id` int(11) DEFAULT NULL,\n  `post_type_id` int(11) DEFAULT NULL,\n  `accepted_answer_id` int(11) DEFAULT NULL,\n  `parent_id` int(11) DEFAULT NULL,\n  `creation_date` datetime DEFAULT NULL,\n  `score` int(11) DEFAULT NULL,\n  `view_count` int(11) DEFAULT NULL,\n  `post_body` varchar(10000) DEFAULT NULL,\n  `owner_user_id` int(11) DEFAULT NULL,\n  `last_editor_user_id` int(11) DEFAULT NULL,\n  `last_edit_date` datetime DEFAULT NULL,\n  `last_activity_date` datetime DEFAULT NULL,\n  `post_title` varchar(500) DEFAULT NULL,\n  `tags` varchar(500) DEFAULT NULL,\n  `answer_count` int(11) DEFAULT NULL,\n  `comment_count` int(11) DEFAULT NULL,\n  `content_license` varchar(100) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n\n--\n-- Table structure for table `tags`\n--\n\nDROP TABLE IF EXISTS `tags`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `tags` (\n  `id` int(11) DEFAULT NULL,\n  `tag_name` varchar(100) DEFAULT NULL,\n  `tag_count` int(11) DEFAULT NULL,\n  `except_post_id` int(11) DEFAULT NULL,\n  `wiki_post_id` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n\n--\n-- Table structure for table `users`\n--\n\nDROP TABLE IF EXISTS `users`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `users` (\n  `id` int(11) DEFAULT NULL,\n  `reputation` int(11) DEFAULT NULL,\n  `creation_date` datetime DEFAULT NULL,\n  `display_name` varchar(200) DEFAULT NULL,\n  `last_access_date` datetime DEFAULT NULL,\n  `website_url` varchar(1000) DEFAULT NULL,\n  `location` varchar(200) DEFAULT NULL,\n  `about_me` varchar(10000) DEFAULT NULL,\n  `views` int(11) DEFAULT NULL,\n  `upvotes` int(11) DEFAULT NULL,\n  `downvotes` int(11) DEFAULT NULL,\n  `account_id` int(11) DEFAULT NULL\n) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n\n--\n-- Table structure for table `votes`\n--\n\nDROP TABLE IF EXISTS `votes`;\n/*!40101 SET @saved_cs_client     = @@character_set_client */;\n/*!40101 SET character_set_client = utf8 */;\nCREATE TABLE `votes` (\n  `id` int(11) DEFAULT NULL,\n  `post_id` int(11) DEFAULT NULL,\n  `vote_type_id` int(11) DEFAULT NULL,\n  `creation_date` datetime DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n/*!40101 SET character_set_client = @saved_cs_client */;\n/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;\n\n/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;\n/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;\n\n-- Dump completed on 2025-01-06 10:20:17\n",
        backup_db_base_directory="/mnt/test2/var/lib/mysql",
        target_db_base_directory="/var/lib/mysql",
    )
    importer.process()
