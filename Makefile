.PHONY: writer reader loadgen build build-poc build-poc-force jni clean-writer-db clean-reader-db

# ---- Cloud defaults (override per environment) --------------------------------
AWS_SDK ?= /opt/homebrew
CLOUD_BUCKET ?= test-bucket
CLOUD_OBJECT_PATH ?= rocksdb-cloud-poc
CLOUD_REGION ?= us-east-1
AWS_ENDPOINT ?= http://localhost:9000
AWS_S3_FORCE_PATH_STYLE ?= true
AWS_ACCESS_KEY_ID ?= minioadmin
AWS_SECRET_ACCESS_KEY ?= minioadmin
CLOUD_COOKIE ?= cloud-poc

WRITER_DB_PATH ?= $(HOME)/temp/rocksdb-cloud-writer
WRITER_HTTP_PORT ?= 8081
WRITER_FLUSH_ON_WRITE ?= false
WRITER_RESYNC_ON_OPEN ?= false
WRITER_ENABLE_PERIODIC_FLUSH ?= true
WRITER_FLUSH_INTERVAL_SECONDS ?= 10

READER_DB_PATH ?= $(HOME)/temp/rocksdb-cloud-reader
READER_HTTP_PORT ?= 8082
READER_RESYNC_ON_OPEN ?= true
READER_ENABLE_PERIODIC_RESYNC ?= true
READER_RESYNC_INTERVAL_SECONDS ?= 10

# ---- Tooling ------------------------------------------------------------------
JAVA_HOME ?= $(shell /usr/libexec/java_home 2>/dev/null)
JAVA ?= $(if $(JAVA_HOME),$(JAVA_HOME)/bin/java,java)
MAVEN ?= mvn
MAVEN_FLAGS ?= -Dmaven.test.skip=true

POC_JAR := rocksdb-sync-poc/target/rocksdb-sync-poc-0.1-SNAPSHOT.jar
ROCKSDB_JAR := rocksdb-cloud/java/target/rocksdbjni-9.1.1-osx.jar
M2_REPO ?= $(HOME)/.m2/repository
SLF4J_VERSION ?= 2.0.16
SLF4J_JARS := $(M2_REPO)/org/slf4j/slf4j-simple/$(SLF4J_VERSION)/slf4j-simple-$(SLF4J_VERSION).jar:$(M2_REPO)/org/slf4j/slf4j-api/$(SLF4J_VERSION)/slf4j-api-$(SLF4J_VERSION).jar
POC_CLASSPATH := $(POC_JAR):$(ROCKSDB_JAR):$(SLF4J_JARS)

ENV_COMMON = env \
	CLOUD_BUCKET=$(CLOUD_BUCKET) \
	CLOUD_OBJECT_PATH=$(CLOUD_OBJECT_PATH) \
	CLOUD_REGION=$(CLOUD_REGION) \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	AWS_ENDPOINT=$(AWS_ENDPOINT) \
	AWS_ENDPOINT_URL=$(AWS_ENDPOINT) \
	AWS_S3_FORCE_PATH_STYLE=$(AWS_S3_FORCE_PATH_STYLE) \
	CLOUD_COOKIE=$(CLOUD_COOKIE)

# ---- Targets ------------------------------------------------------------------
writer: $(ROCKSDB_JAR) $(POC_JAR)
	@echo "Starting writer server on port $(WRITER_HTTP_PORT) (Ctrl+C to stop)..."
	@$(ENV_COMMON) \
		LOCAL_DB_PATH=$(WRITER_DB_PATH) \
		WRITER_HTTP_PORT=$(WRITER_HTTP_PORT) \
		WRITER_FLUSH_ON_WRITE=$(WRITER_FLUSH_ON_WRITE) \
		WRITER_ENABLE_PERIODIC_FLUSH=$(WRITER_ENABLE_PERIODIC_FLUSH) \
		WRITER_FLUSH_INTERVAL_SECONDS=$(WRITER_FLUSH_INTERVAL_SECONDS) \
		RESYNC_ON_OPEN=$(WRITER_RESYNC_ON_OPEN) \
		$(JAVA) -cp "$(POC_CLASSPATH)" com.slack.rocksdbsync.writer.WriterServerMain

reader: $(ROCKSDB_JAR) $(POC_JAR)
	@echo "Starting reader server on port $(READER_HTTP_PORT) (Ctrl+C to stop)..."
	@$(ENV_COMMON) \
		LOCAL_DB_PATH=$(READER_DB_PATH) \
		READER_HTTP_PORT=$(READER_HTTP_PORT) \
		RESYNC_ON_OPEN=$(READER_RESYNC_ON_OPEN) \
		READER_ENABLE_PERIODIC_RESYNC=$(READER_ENABLE_PERIODIC_RESYNC) \
		READER_RESYNC_INTERVAL_SECONDS=$(READER_RESYNC_INTERVAL_SECONDS) \
		$(JAVA) -cp "$(POC_CLASSPATH)" com.slack.rocksdbsync.reader.ReaderServerMain

loadgen: $(POC_JAR)
	@echo "Running KV loadgen against WRITER_URL and READER_URL..."
	@$(ENV_COMMON) \
		$(JAVA) -cp "$(POC_CLASSPATH)" com.slack.rocksdbsync.loadgen.KVLoadGen

# Build only the RocksDB JNI jar (no POC rebuild)
jni: $(ROCKSDB_JAR)
	@echo "JNI build complete: $(ROCKSDB_JAR)"

clean-writer-db:
	@rm -rf "$(WRITER_DB_PATH)"

clean-reader-db:
	@rm -rf "$(READER_DB_PATH)"

# ---- Build artifacts ----------------------------------------------------------
$(POC_JAR):
	$(MAVEN) $(MAVEN_FLAGS) -pl rocksdb-sync-poc clean package

$(ROCKSDB_JAR):
	cd rocksdb-cloud && env JAVA_HOME=$(JAVA_HOME) AWS_SDK=$(AWS_SDK) USE_AWS=1 DISABLE_JAVA_TESTS=1 $(MAKE) rocksdbjava

# ---- Convenience builds ------------------------------------------------------
build: $(ROCKSDB_JAR) $(POC_JAR)
	@echo "Build complete: $(POC_JAR) and $(ROCKSDB_JAR)"

build-poc: $(POC_JAR)
	@echo "Build complete: $(POC_JAR)"

# Force rebuild only the POC jar (no JNI).
build-poc-force:
	$(MAVEN) $(MAVEN_FLAGS) -pl rocksdb-sync-poc clean package
	@echo "Force build complete: $(POC_JAR)"
