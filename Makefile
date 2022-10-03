DOCKER_IMAGE=nsqio/nsq:v1.2.1
POM_VERSION:=$(shell mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -q -DforceStdout)
SCRIPTS_DIR=scripts

all: setup test docker_teardown target/nsq-j-$(POM_VERSION).jar

target/%.jar:
	mvn package

setup:
	docker pull $(DOCKER_IMAGE)

test:
	mvn verify

deploy:
	mvn --no-transfer-progress --batch-mode deploy

docker_teardown:
	$(SCRIPTS_DIR)/testCleanupDocker.sh

clean: docker_teardown
	mvn clean

.PHONY: clean docker_teardown test setup
