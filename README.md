# Guide Spark Mesos integration tests

The goal of this project to provide adequate test suite to test Spark Mesos integration and verify supported
mesos features are working properly for Spark. Ideally these tests should be run again very Spark pull request before they are merged to master.


## Prerequisite 

This project assumes that you have docker based mesos cluster running locally.

Please use the [spark-ops-docker](https://github.com/typesafehub/spark-ops-docker/) project to setup your mesos cluster.

#### Currently it only supports local docker mode. In future we will add EC2 support.

## Configure the project

Update the **application.conf** file under *src/main/resources* with respective values before running the tests. 

##Running the tests

Invoke the following sbt task by specifying the SPARK_HOME folder and mesos master url

```sh
cd mesos-spark-integration-tests
sbt "mit <spark_home> mesos://<mesos-master-ip>:5050"
```

*We use spark_submit.sh from <SPARK_HOME> to submit jobs. So please make sure you have spark binary distribution download and unzip*
