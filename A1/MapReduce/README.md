# How to run the job

## 1. Package a jar file

Before packaging a jar file, the 8th line in ```build.xml``` file:
```
<fileset dir="/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/">
```
should be replaced with a real hadoop path such as following path on the lab machine:
```
<fileset dir="${env.HADOOP_HOME}/share/hadoop/">
```
Then, we need to run ``` ant ``` in the directory where ```build.xml``` file resides.

## 2. Run in terminal:

Run following command in the directory where ```workload1.jar``` file resides.
```
hadoop jar workload1.jar workload1.Main <input> <output>
```

Replace **< input >** and **< output >** with the real path of input file and output file.
