# OpenMediation Data Task

OpenMediation Data Task is a Data Process Center.  
This service should run singleton.  
It provide api and pb data for [OM-Server](https://github.com/AdTiming/OM-Server)


## Usage

### Packaging

You can package using [mvn](https://maven.apache.org/).

```
mvn clean package -Dmaven.test.skip=true
```

After packaging is complete, you can find "on-dtask.jar" in the directory "target".  
"om-dtask.jar" is a executable jar, see [springboot](https://spring.io/projects/spring-boot/)


### Configuration

"om-dtask.conf"

```shell script
## springboot cfg ###
MODE=service
APP_NAME=om-dtask
#JAVA_HOME=/usr/local/java/jdk
JAVA_OPTS="-Dapp=$APP_NAME\
 -Duser.timezone=UTC\
 -Xmx3g\
 -Xms1g\
 -server"

RUN_ARGS="--spring.profiles.active=prod"
PID_FOLDER=log
LOG_FOLDER=log
LOG_FILENAME=stdout.log
```

### Run

put "om-dtask.conf" and "om-dtask.jar" in the same directory.

```
├── on-dtask.conf
├── on-dtask.jar
└── log
```

```shell script
mkdir -p log
./om-dtask.jar start
```

### Logs

```shell script
tail -f log/stdout.log
```

### Stop

```shell script
./om-dtask.jar stop
```

### Restart

```shell script
./om-dtask.jar restart
```


