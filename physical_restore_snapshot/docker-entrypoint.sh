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

		# Execute the command
		# mysql_note "Opening Shell"
		# exec /bin/bash

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
    fi

    # If not running mariadbd, just execute the command
    exec "$@"
}

_main "$@"