Directions are [here](https://medium.com/@pedrodc/setting-up-a-spark-machine-learning-project-with-scala-sbt-and-mllib-831c329907ea)

Get sbt, if not already installed.
```
$ echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
$ sudo apt-get update
$ sudo apt-get install sbt
```

Create new project.
```
$ sbt new sbt/scala-seed.g8
$ cd [my-project]
$ sbt
$ sbt:myproject> run
```

Add the following to build.sbt.
```
val sparkCore = "org.apache.spark" %% "spark-core" %% "2.4.0"
val sparkMLlib = "org.apache.spark" %% "spark-mllib" %% "2.4.0"

libraryDependencies += sparkCore,
libraryDependencies += sparkMLlib
```



