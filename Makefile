.PHONY: start-writer stop-writer start-reader stop-reader loadgen-smoke loadgen-full

# Common defaults (override in environment if needed)
S3_BUCKET ?= test-bucket
S3_PREFIX ?= rocksdb-sync-poc
S3_ENDPOINT ?= http://localhost:9000
S3_REGION ?= us-east-1
AWS_ACCESS_KEY_ID ?= minioadmin
AWS_SECRET_ACCESS_KEY ?= minioadmin

WRITER_DB_DIR ?= $(HOME)/temp/rocksdb-writer
WRITER_SNAPSHOT_DIR ?= $(HOME)/temp/snapshot-writer
WRITER_HTTP_PORT ?= 8081
BACKUP_INTERVAL_SECONDS ?= 30

READER_DB_DIR ?= $(HOME)/temp/rocksdb-reader
READER_HTTP_PORT ?= 8082
RESTORE_INTERVAL_SECONDS ?= 30

define RUN_WRITER
env WRITER_DB_PATH=$(WRITER_DB_DIR) \
    WRITER_SNAPSHOT_PATH=$(WRITER_SNAPSHOT_DIR) \
    S3_BUCKET=$(S3_BUCKET) \
    S3_PREFIX=$(S3_PREFIX) \
    S3_ENDPOINT=$(S3_ENDPOINT) \
    S3_REGION=$(S3_REGION) \
    AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
    AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
    WRITER_HTTP_PORT=$(WRITER_HTTP_PORT) \
    BACKUP_INTERVAL_SECONDS=$(BACKUP_INTERVAL_SECONDS) \
    mvn -pl rocksdb-sync-poc exec:java -Dexec.mainClass=com.slack.rocksdbsync.writer.WriterMain
endef

define RUN_READER
env READER_DB_PATH=$(READER_DB_DIR) \
    S3_BUCKET=$(S3_BUCKET) \
    S3_PREFIX=$(S3_PREFIX) \
    S3_ENDPOINT=$(S3_ENDPOINT) \
    S3_REGION=$(S3_REGION) \
    AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
    AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
    READER_HTTP_PORT=$(READER_HTTP_PORT) \
    RESTORE_INTERVAL_SECONDS=$(RESTORE_INTERVAL_SECONDS) \
    mvn -pl rocksdb-sync-poc exec:java -Dexec.mainClass=com.slack.rocksdbsync.reader.ReaderMain
endef

start-writer:
	@mkdir -p logs
	@echo "Starting writer service..."
	@echo "  WRITER_DB_PATH=$(WRITER_DB_DIR)"
	@echo "  WRITER_SNAPSHOT_PATH=$(WRITER_SNAPSHOT_DIR)"
	@sh -c '$(RUN_WRITER) > logs/writer.log 2>&1 & echo $$! > .writer.pid'
	@echo "Writer PID $$(cat .writer.pid) (logs/writer.log)"

stop-writer:
	@if [ -f .writer.pid ]; then \
		echo "Stopping writer PID $$(cat .writer.pid)"; \
		kill $$(cat .writer.pid) && rm -f .writer.pid; \
	else \
		echo "Writer PID file not found"; \
	fi

start-reader:
	@mkdir -p logs
	@echo "Starting reader service..."
	@echo "  READER_DB_PATH=$(READER_DB_DIR)"
	@sh -c '$(RUN_READER) > logs/reader.log 2>&1 & echo $$! > .reader.pid'
	@echo "Reader PID $$(cat .reader.pid) (logs/reader.log)"

stop-reader:
	@if [ -f .reader.pid ]; then \
		echo "Stopping reader PID $$(cat .reader.pid)"; \
		kill $$(cat .reader.pid) && rm -f .reader.pid; \
	else \
		echo "Reader PID file not found"; \
	fi

loadgen-smoke:
	@echo "Running loadgen smoke test (100 keys)..."
	@LOADGEN_TOTAL_BATCHES=1 \
	 LOADGEN_KEYS_PER_BATCH=100 \
	 LOADGEN_KEY_SIZE_BYTES=32 \
	 LOADGEN_VALUE_SIZE_BYTES=1024 \
	 LOADGEN_WRITER_URL=http://localhost:$(WRITER_HTTP_PORT)/kv \
	 LOADGEN_READER_URL=http://localhost:$(READER_HTTP_PORT) \
	 mvn -pl rocksdb-sync-poc exec:java \
	   -Dexec.mainClass=com.slack.rocksdbsync.loadgen.LoadGeneratorMain

loadgen-full:
	@echo "Running full 10GiB loadgen..."
	@LOADGEN_TOTAL_BATCHES=10 \
	 LOADGEN_BATCH_BYTES=$$((1024*1024*1024)) \
	 LOADGEN_KEY_SIZE_BYTES=32 \
	 LOADGEN_VALUE_SIZE_BYTES=1024 \
	 LOADGEN_WRITER_URL=http://localhost:$(WRITER_HTTP_PORT)/kv \
	 LOADGEN_READER_URL=http://localhost:$(READER_HTTP_PORT) \
	 mvn -pl rocksdb-sync-poc exec:java \
	   -Dexec.mainClass=com.slack.rocksdbsync.loadgen.LoadGeneratorMain
