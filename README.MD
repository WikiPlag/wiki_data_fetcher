# Wiki Data Fetcher

* fetches Wikipedia documents ([download](https://dumps.wikimedia.org/dewiki/))
* prepares the data
* stores it into the database

## Get XML Dump
```bash
WIKI_FILE=https://dumps.wikimedia.org/dewiki/..........-pages-articles-multistream.xml.bz2
HADOOP_PATH=hdfs://...

# download zip file 
curl $WIKI_FILE | hadoop fs -put - $HADOOP_PATH/wiki.bz2 

# extract file to xml
hadoop fs -text $HADOOP_PATH/wiki.bz2 | hadoop fs -put - $HADOOP_PATH/wiki.xml
```

## Build
```bash
sbt clean assembly
```

## Commands
```bash
spark-submit $SPARK_OPTIONS wiki_data_fetcher.jar $OPTIONS   
```
```bash
$SPARK_OPTIONS are:
--executor-memory 5G --driver-memory 2G
```
see [Docs](https://spark.apache.org/docs/latest/submitting-applications.html)

```bash
$OPTIONS are: 

Options:
 -e, --extract <hadoop_file>          parse wiki XML file and saves it 
 -i, --index                          use db-entries to create an inverse index 
 -n, --ngrams <ngram>                 use db-entries to create hashed n-grams of a given size 
 -h, --help
 -m, --mongodb_path <path>            MongoDB Path
 -p, --mongodb_port <port>            MongoDB Port
 -pw,--mongodb_pass <pass>            MongoDB Password
 -u, --mongodb_user <user>            MongoDB User
```
e.g.
```bash
wiki_data_fetcher.jar -e $HADOOP_PATH/wiki.xml -m mongoPath  -p mongoPort -u mongoUser -pw mongoPassword
```
