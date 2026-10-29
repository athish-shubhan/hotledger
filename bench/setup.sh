#!/usr/bin/env bash
# One-time setup for the designs that need more than Postgres and Redis.
#   - TigerBeetle: the release binary (design 6), on PATH as `tigerbeetle`.
#   - MySQL 8 (apt package mysql-server): a dedicated data directory for the
#     mysqld the orchestrator starts on port 3307 (it never touches the
#     system instance).
set -euo pipefail
TB_DIR=${TB_DIR:-/opt/tigerbeetle}
if ! command -v tigerbeetle >/dev/null; then
  mkdir -p "$TB_DIR"
  curl -sSL -o "$TB_DIR/tb.zip" https://github.com/tigerbeetle/tigerbeetle/releases/download/0.17.9/tigerbeetle-x86_64-linux.zip
  (cd "$TB_DIR" && unzip -o tb.zip && rm tb.zip)
  ln -sf "$TB_DIR/tigerbeetle" /usr/local/bin/tigerbeetle
fi
tigerbeetle version
mkdir -p /var/lib/hotledger-tb

D=${MYSQL_DATADIR:-/var/lib/hotledger-mysql}
if [[ ! -d $D/mysql ]]; then
  command -v mysqld >/dev/null || DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server
  mkdir -p "$D" /run/hotledger-mysql
  mysqld --no-defaults --initialize-insecure --user=root --datadir="$D"
  mysqld --no-defaults --user=root --datadir="$D" --port=3307 --socket=/run/hotledger-mysql/mysqld.sock --mysqlx=OFF --skip-name-resolve &
  pid=$!
  for _ in $(seq 60); do mysqladmin -uroot -S /run/hotledger-mysql/mysqld.sock ping >/dev/null 2>&1 && break; sleep 1; done
  mysql -uroot -S /run/hotledger-mysql/mysqld.sock -e "CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY ''; GRANT ALL ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION; CREATE DATABASE IF NOT EXISTS hotledger;"
  kill $pid; wait $pid || true
fi
mkdir -p /run/hotledger-mysql
echo "setup done"
