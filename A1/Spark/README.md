# How to run the job

## 1. Package a jar file

Before packaging a jar file, the 7th line in ```build.xml``` file:
```
<fileset dir="/usr/local/Cellar/spark-2.4.1-bin-hadoop2.7/jars">
```
should be replaced with a real spark path such as following path on the lab machine:
```
<fileset dir="${env.SPARK_HOME}/jars">
```
Then, run ``` ant ``` in the directory where ```build.xml``` file resides.

## 2. Run in terminal:

Run following command in the directory where ```workload2.jar ``` file resides.
```
spark-submit --class workload2.Main --master local[1] workload2.jar <input> <output>
```

Replace **< input >** and **< output >** with the real path of input file and output file.
