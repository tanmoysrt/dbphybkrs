#!/bin/bash
set -eo pipefail

# Logging functions
mysql_log() {
    local type="$1"; shift
    printf '%s [%s] [Entrypoint]: %s\n' "$(date --rfc-3339=seconds)" "$type" "$*"
}
mysql_note() {
    mysql_log Note "$@"
}
mysql_error() {
    mysql_log ERROR "$@" >&2
    exit 1
}

# Get config values from mariadbd
mysql_get_config() {
    local conf="$1"; shift
    "$@" --verbose --help --log-bin-index="$(mktemp -u)" 2>/dev/null \
        | awk -v conf="$conf" '$1 == conf && /^[^ \t]/ { sub(/^[^ \t]+[ \t]+/, ""); print; exit }'
}

# Check if mariadbd can start with the provided config
mysql_check_config() {
    local toRun=( "$@" --verbose --help --log-bin-index="$(mktemp -u)" )
    if ! errors="$("${toRun[@]}" 2>&1 >/dev/null)"; then
        mysql_error "Failed to check config. Command was: ${toRun[*]} ($errors)"
    fi
}

# Setup basic environment
docker_setup_env() {
    # Get config
    declare -g DATADIR SOCKET
    DATADIR="$(mysql_get_config 'datadir' "$@")"
    SOCKET="$(mysql_get_config 'socket' "$@")"

    # Ensure data directory exists and has correct permissions
    mkdir -p "$DATADIR"
    find "$DATADIR" \! -user mysql -exec chown mysql: '{}' +
    
    # Handle socket directory permissions
    if [ "${SOCKET:0:1}" != '@' ]; then # not abstract socket
        find "${SOCKET%/*}" -maxdepth 0 \! -user mysql -exec chown mysql: '{}' \;
    fi
}

_main() {
    # If command starts with an option, prepend mariadbd
    if [ "${1:0:1}" = '-' ]; then
        set -- mariadbd "$@"
    fi

    # raise error if /target_db not exists
    if [ ! -d /target_db ]; then
        mysql_error "Directory /target_db does not exist. Please mount the target_db volume."
    fi

    # raise error if /var/lib/mysql not exists
    if [ ! -d /var/lib/mysql ]; then
        mysql_error "Directory /var/lib/mysql does not exist. Please mount the mysql volume."
    fi

    # check for MYSQL_UID and MYSQL_GID environment variables
    if [ -z "${MYSQL_UID}" ] || [ -z "${MYSQL_GID}" ]; then
        mysql_error "MYSQL_UID and MYSQL_GID environment variables must be set."
    fi

    # check for BACKUP_DB environment variable
    if [ -z "${BACKUP_DB}" ]; then
        mysql_error "BACKUP_DB environment variable must be set."
    fi
    # check for TARGET_DB environment variable
    if [ -z "${TARGET_DB}" ]; then
        mysql_error "TARGET_DB environment variable must be set."
    fi
    # check for TARGET_DB_ROOT_PASSWORD environment variable
    if [ -z "${TARGET_DB_ROOT_PASSWORD}" ]; then
        mysql_error "TARGET_DB_ROOT_PASSWORD environment variable must be set."
    fi
    # check for BACKUP_DB_ROOT_PASSWORD environment variable
    if [ -z "${BACKUP_DB_ROOT_PASSWORD}" ]; then
        mysql_error "BACKUP_DB_ROOT_PASSWORD environment variable must be set."
    fi

    # Set the UID and GID for the mysql user
    groupmod -g ${MYSQL_GID} mysql
    usermod -u ${MYSQL_UID} -g ${MYSQL_GID}  mysql

    # Only process if running mariadbd/mysqld
    if [ "$1" = 'mariadbd' ] || [ "$1" = 'mysqld' ]; then
        mysql_note "Entrypoint script for MariaDB Server started."

        # Basic config check
        mysql_check_config "$@"
        
        # Setup environment and directories
        docker_setup_env "$@"

        # Switch to mysql user and execute
        mysql_note "Switching to dedicated user 'mysql'"
        exec gosu mysql "$@" &
		MYSQL_PID=$!

		mysql_note "Running at PID: $MYSQL_PID"

		# Wait for mariadbd to start
		mysql_note "Waiting for MySQL to be ready"
        while ! mysqladmin ping --silent; do
            sleep 1
        done
        mysql_note "MySQL is ready."

		# Execute the restore script
        python3 /restore.py
        # sleep infinity

		# Try to stop mariadbd
		mysql_note "Stopping MySQL"
		kill -s TERM $MYSQL_PID 2> /dev/null

		# Wait for mariadbd to stop
		mysql_note "Waiting for MySQL to stop"
		while kill -0 $MYSQL_PID 2> /dev/null; do
			echo "Waiting for MySQL to stop"
			sleep 1
		done
		mysql_note "MySQL has stopped."
    else
        mysql_note "Running command: $@"
        exec "$@"
    fi
}

_main "$@"