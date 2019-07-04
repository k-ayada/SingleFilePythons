#import os
#os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
from __future__          import print_function

import sys,logging,boto3,json
from botocore.exceptions import ClientError
from collections         import OrderedDict

from pyspark             import SparkConf
from pyspark.sql         import SparkSession
from pyspark.sql.types   import StringType,DateType,TimestampType
from pyspark.sql.types   import IntegerType, LongType, ShortType, DecimalType,DoubleType,FloatType

s3Res   = boto3.resource('s3')
s3      = boto3.client('s3')
ssm     = boto3.client('ssm')
glue    = boto3.client('glue')
parms   = OrderedDict()
mainSQL = OrderedDict()
log     = logging.getLogger(__name__)

def argsToDict(argv):
    '''
        Convert the program argumens in list to dict
        For key-value bsed parms use format --key value
        For flag based parms you  dont prepend '--'
        Example `argsToDict(['--key' , 'value' , '-flag'])`
        Retuns
        `{'-key' : 'value' , '-flag': '-flag' }`
    '''
    i = 1
    while i <= len(argv) -1:
        if ("--" == argv[i][0:2]):
            parms[argv[i]] = argv[i+1]
            i = i + 1
        else:
            parms[argv[i]] = argv[i]
        i = i + 1

def setupLogger():
    '''
    Inits the python logging object
    '''
    handler   = logging.StreamHandler(sys.stdout)
    dfmt = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(module)s.%(funcName)s[%(lineno)03d] %(message)s',dfmt)
    handler.setFormatter(formatter)
    lvl = parms["--logLevel"]
    if lvl == 'i':
        log.setLevel(logging.INFO)
    elif lvl == 'w':
        log.setLevel(logging.WARN)
    elif lvl == 'd':
        log.setLevel(logging.DEBUG)
    elif lvl == 'e':
        log.setLevel(logging.ERROR)
    elif lvl == 'c':
        log.setLevel(logging.CRITICAL)
    else :
        log.setLevel(logging.INFO)
    log.addHandler(handler)

    log.info("Setting log level to {}".format(logging.getLevelName(log.getEffectiveLevel())))

def handleSSM():
    ''''
    if one of the parameter entry point to ssm (by prepending 'ssm:'). Calls ssm to retrive the value
    '''
    for key in parms:
        if parms[key].startswith("ssm:/"):
            parms[key] = getFromSSM(parms[key])

def reportRunTimeParms():
    msg = ""
    for i, key in enumerate(parms):
        if key == "--password" and ( not parms[key].startswith("ssm")):
            msg += "\t %2d. %20s -> ********\n" %(i, key )
        else :
            msg +="\t %2d. %20s -> %s\n" %(i, key, parms[key] )

    log.info("Starting the processing with below parms,\n {}\n\n".format(msg))

def validateArg(key) :
    '''
        If the input key is missing in the dict-> parms, log an error and exit from the program
    '''
    if key not in parms:
        log.info("Please pass a valid value for %s" %(key))
        exit(1)

def validateArgs() :
    '''
       Validates if below parms are received.
       Mandetory: `--srcDB`, `--srcTable`, `--user`, `--password`, `--jdbcUrl` and `--driver`
       Below parms will be defaluted.
       `--writeMode`         `'overwrite' if "-fullLoad" else 'append'` \n
       `--writePartitionCol` `''` \n
       `--logLevel`           `'d'`\n
       `--outFlFmt`          `'parquet'`\n
       `--jdbcReadSplits`    `10`\n
       `--dstDB`             `--srcDB`\n
       `--dstTable`          `--srcTable`\n
    '''
    validateArg("--srcDB")
    validateArg("--srcTable")

    validateArg("--user")
    validateArg("--password")
    validateArg("--jdbcUrl")
    validateArg("--driver")

    if "--writeMode" not in parms :
        parms["--writeMode"] = 'overwrite' if "-fullLoad" in parms else 'append'

    if "--logLevel"   not in parms : parms["--logLevel"]   = 'd'

    if "--outFlFmt"   not in parms : parms["--outFlFmt"]   = 'parquet'
    if "--jdbcReadSplits" not in parms : parms["--jdbcReadSplits"] = 10

    if "--dstDB"    not in parms :
        parms["--dstDB"]   = parms["--srcDB"].upper()
    if "--dstTable" in parms :
        parms["--dstTable"]= parms["--dstTable"].upper()
    else:
        parms["--dstTable"]= parms["--srcTable"].upper()

    if "--writePartitionCol" in parms :
        parms["--writePartitionCol"] = parms["--writePartitionCol"].upper()

def buidMainSQL():
    '''
        Creatses the main sql using below parms,
         Mandetory : --srcDB, --srcTable
         Optional  : --fltrCol, --fltrLBCond, --fltrLB, --fltrUBCond, --fltrUB
    '''
    mainSQL['from'] = "from {}.{}".format(parms["--srcDB"],parms["--srcTable"])
    flter = " "
    if "-fullLoad" not in parms or "--fltrLB" in parms or  "--fltrUB" in parms:
        filterLB,filterUB = "",""
        if "--fltrLB" in parms:
            filterLB = " where {} {} {}".format( parms["--fltrCol"],parms["--fltrLBCond"],parms["--fltrLB"])

        if "--fltrUB" in parms:
            whereOrAnd = "and" if "--fltrLB" in parms else "where"
            filterUB = " {0} {1} {2} {3} ".format(whereOrAnd, parms["--fltrCol"],parms["--fltrUBCond"],parms["--fltrUB"])

        flter =  filterLB +  filterUB

    mainSQL['filter'] = flter

    colList = "*" if "--selctCols" not in parms else parms["--selctCols"]
    sql  =  """
            select /*+ ALL_ROWS */ %s
             %s %s
            """ % (colList, mainSQL['from'], flter)
    log.info("Built main SQL using the below options,\n %s" %  (pritifyDict(mainSQL)) )
    log.info("Main SQL,\n %s" %  ( sql ))
    return sql

def reOrderColsBasedOnWritePartitions(spark,selctCols):
    noRowSQL = """
                select %s
                  %s
                 where 1 <> 1
    """ % (selctCols, mainSQL['from'])
    cols = spark.read \
                .format("jdbc") \
                .option("fetchsize" , "1") \
                .option("user"      , parms["--user"]) \
                .option("password"  , parms["--password"])  \
                .option("driver"    , parms["--driver"] ) \
                .option("url"       , parms["--jdbcUrl"]) \
                .option("dbtable"   , " ( {} ) x".format(noRowSQL)) \
                .load().schema.names
    partCols = list ( map(lambda c: c.strip().upper() , parms["--writePartitionCol"].split(",") ))
    def removeIfExists(col):
        if col in partCols :
            return False
        else :
            return True
    cols = list(filter(removeIfExists,cols))
    for c in partCols :
        cols.append(c)
    log.debug("Reorederd columns,{}".format("\n\t".join(cols)))
    return cols

def readFromRDBMS(spark):
    selctCols = "*" if "--selctCols" not in parms else parms["--selctCols"]
    if "--writePartitionCol" in parms :
        selctCols = " , ".join(reOrderColsBasedOnWritePartitions(spark,selctCols))

    mainSQLStr =  """
            select /*+ ALL_ROWS */
                   %s
              %s
            %s
            """ % (selctCols, mainSQL['from'], mainSQL['filter'])
    log.info("Built main SQL using the below options,\n %s" %  (pritifyDict(mainSQL)) )
    log.info("Main SQL,\n %s" %  ( mainSQLStr ))
    jdbcReader = spark.read \
                      .format("jdbc") \
                      .option("fetchsize" , "10000") \
                      .option("user"      , parms["--user"]) \
                      .option("password"  , parms["--password"]) \
                      .option("driver"    , parms["--driver"] ) \
                      .option("url"       , parms["--jdbcUrl"]) \
                      .option("dbtable"   ," ( {} ) x".format(mainSQLStr))

    jdbcReader = updateFetchPartionInfo(spark,jdbcReader)

    return jdbcReader.load().persist() \
         if "-cache2HDFS" in parms \
         else jdbcReader.load()

def getFromSSM(keyIn):
    '''
    Gets the value from AWS Parameter Store
    '''
    key = keyIn[4:] if keyIn.startswith("ssm:/") else keyIn
    log.info("Getting the value for %s" % (key))
    val = ssm.get_parameter(Name=key, WithDecryption=True)['Parameter']['Value']
    return val

def getBucketNKeyTuple(uriStr: str) -> (str,str)  :
    splt = uriStr.replace("s3://","").split("/")
    bkt = splt.pop(0)
    key = "/".join(splt)
    return (bkt,key)

def pritifyDict(d) :
    return json.dumps(d, indent=4, sort_keys=True, default=str)

def deleteS3Object(bucket, key):
    if not key.endswith("/")  :
        key = "%s/" % (key)

    keyLst =  s3Res.Bucket(bucket).objects.filter(Prefix="%s" % (key))
    for i,k in enumerate(keyLst):
        log.debug("%3d Deleting s3 Object: %s/%s" % (i,bucket,k))
        s3.delete_object(Bucket=k.bucket_name,Key=k.key)

def getTableMeataDatFromGlue(db,table):
    res = None
    log.debug("Gettign the Glue meta-data for  {0}.{1}".format(db,table))
    try:
        res = glue.get_table(DatabaseName=db, Name=table)
        log.debug("Glue Meta-data retrieved, {}".format(pritifyDict(res)))
        return res['Table']
    except ClientError as ex:
        if ex.response['Error']['Code'] == "EntityNotFoundException":
            log.warn("Table not found. Nothing to delete. Message from Glue Service: {}".format (str(ex)))
        return None
    except:
        log.error("Unexpected error. Treating asif Table not present:{}".format(sys.exc_info()))
        return None

def markForDelete(glueMetaData):
    try :
        (bkt,key) = getBucketNKeyTuple(glueMetaData['StorageDescriptor']['Location'])
        deleteS3Object(bkt,key)
        glue.delete_table(DatabaseName=glueMetaData['DatabaseName'], Name=glueMetaData['Name'])
    except:
        log.error("Unexpected error. Treating asif Table not present:{}".format(sys.exc_info()))
        return False

    return True

def generateSparkSession(appName):
    #https://spark.apache.org/docs/latest/cloud-integration.html
    hmConf = {
        "spark.executor.pyspark.memory"                  : "512m",
        "spark.debug.maxToStringFields"                  : "5000",
        "spark.rps.askTimeout"                           : "1200",
        "spark.network.timeout"                          : "1200",

        "spark.maxRemoteBlockSizeFetchToMem"             : "512m",
        "spark.broadcast.blockSize"                      : "16m",
        "spark.broadcast.compress"                       : "true",
        "spark.rdd.compress"                             : "true",
        "spark.io.compression.codec"                     : "org.apache.spark.io.SnappyCompressionCodec",

        "spark.kryo.unsafe"                              : "true",
        "spark.serializer"                               : "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer"                    : "10240",
        "spark.kryoserializer.buffer.max"                : "2040m",
        "hive.exec.dynamic.partition"                    : "true",
        "hive.exec.dynamic.partition.mode"               : "nonstrict",
        "hive.warehouse.data.skiptrash"                  : "true",

        "spark.sql.hive.metastorePartitionPruning"       : "true",

        "fs.s3a.fast.upload"                             : "true",
        "fs.s3.buffer.dir"                               : "/mnt/tmp/s3",

        "spark.sql.broadcastTimeout"                                    : "1200",
        "spark.sql.sources.partitionOverwriteMode"                      : "dynamic",

        "spark.sql.orc.filterPushdown"                                  : "true",
        "spark.sql.orc.splits.include.file.footer"                      : "true",
        "spark.sql.orc.cache.stripe.details.size"                       : "1000",

        "spark.hadoop.parquet.enable.summary-metadata"                  : "false",
        "spark.sql.parquet.mergeSchema"                                 : "false",
        "spark.sql.parquet.filterPushdown"                              : "true",
        "spark.sql.parquet.fs.optimized.committer.optimization-enabled" : "true",
        "spark.sql.parquet.output.committer.class"                      : "com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter",

        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"       :"2",
        "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true"

    }
    sparkConf = SparkConf()

    for (k,v) in hmConf.items():
        sparkConf.set(k,v)

    spark = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .appName(appName or "PySparkApp") \
            .enableHiveSupport() \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4",  "true")
    sc.setSystemProperty("com.amazonaws.services.s3.enforceV4", "true")
    sc.setLogLevel(parms.get("--sparklogLvl", "INFO"))

    #from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher
    #provider = InstanceMetadataProvider(iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2))
    #creds = provider.load()
    #session = boto3.Session(region_name=parms.get("--awsRegion", "us-east-1"))
    #creds   = session.get_credentials().get_frozen_credentials()
    #log.debug("boto2: {}".format(creds))

    hdpCnf = sc._jsc.hadoopConfiguration()

    hdpCnfhm = {
        "io.file.buffer.size"                             : "65536",
        "mapreduce.fileoutputcommitter.algorithm.version" : "2",
        #"fs.s3a.access.key"                               : creds.access_key,
        #"fs.s3a.secret.key"                               : creds.secret_key,
        #"fs.s3a.server-side-encryption-algorithm"         : "SSE-KMS",
        #"fs.s3.enableServerSideEncryption"                : "true",
        #"fs.s3.impl"                                      : "org.apache.hadoop.fs.s3a.S3AFileSystem",
        #"fs.s3a.impl"                                     : "org.apache.hadoop.fs.s3a.S3AFileSystem",
        #"fs.s3a.endpoint"                                 : "s3.%s.amazonaws.com" % (parms.get("--awsRegion", "us-east-1"))
    }

    for (k,v) in hdpCnfhm.items():
        hdpCnf.set(k,v)

    msg = ""
    for k in sc._conf.getAll():
        msg += "\t%50s -> %s\n" % (k[0], k[1])

    log.info("Initiated SparkSesion with below confs,\n{}".format(msg))

    return spark

def getMinMax(spark):
    filterExp = mainSQL['filter']
    if " where " in filterExp :
        filterExp += "  and {0} is not null".format(parms["--fetchPartitionCol"])
    else :
        filterExp += "  where {0} is not null".format(parms["--fetchPartitionCol"])

    SQL = """
          select  'min' as TYP , min({0}) as VAL
            {1}
          {2}
           union all
          select  'max' as TYP , max({0}) as VAL
            {1}
          {2}
          """.format(parms["--fetchPartitionCol"],mainSQL['from'],filterExp)

    log.info("Fetching min amd max value using the sql: \n\t%s" %  (SQL) )

    min_max =  spark.read \
                    .format("jdbc") \
                    .option("fetchsize" , "2")   \
                    .option("user"      , parms["--user"]) \
                    .option("password"  , parms["--password"]) \
                    .option("driver"    , parms["--driver"] ) \
                    .option("url"       , parms["--jdbcUrl"]) \
                    .option("dbtable"   ," ( {} ) x".format(SQL)) \
                    .load().collect()

    (mn , mx) = (None, None)
    (mn , mx) =     (min_max[0]['VAL'] , min_max[1]['VAL'])  if min_max[0]['TYP'] == 'min' \
               else (min_max[1]['VAL'] , min_max[0]['VAL'])

    log.info("min({0}): {1} , max({0}):{2}"\
                .format(parms["--fetchPartitionCol"], mn, mx) )

    if mn == None or mx == None :
        log.error("Not able to find the min and max vales (got Null result)." )
        log.error("Returned Records are, \n\t{}".format(min_max))
        return  (None, None)
    return (int(mn), int(mx))

def getMinOrMax(spark, typ):
    filterExp = mainSQL['filter']
    if " where " in filterExp :
        filterExp += "  and {0} is not null".format(parms["--fetchPartitionCol"])
    else :
        filterExp += "  where {0} is not null".format(parms["--fetchPartitionCol"])

    SQL = """
    select {0}({1}) as VAL
       {2}
       {3}
    """.format(typ,parms["--fetchPartitionCol"], mainSQL['from'],filterExp)

    log.info("Fetching %s value using the sql: \n\t%s" %  (typ,SQL) )

    min_max =  spark.read \
                    .format("jdbc") \
                    .option("fetchsize" , "2")   \
                    .option("user"      , parms["--user"]) \
                    .option("password"  , parms["--password"]) \
                    .option("driver"    , parms["--driver"] ) \
                    .option("url"       , parms["--jdbcUrl"]) \
                    .option("dbtable"   ," ( {} ) x".format(SQL)) \
                    .load().collect()

    val =  min_max[0]['VAL']
    log.info("Derived {0}({1}): {2}"\
                .format(typ,parms["--fetchPartitionCol"], val) )

    if val == None :
        log.error("Not able to find the {0} vales (got Null result).".format(typ) )
        log.error("Returned Records are, \n\t{}".format(min_max))
        return None
    else:
        return int(val)

def updateFetchPartionInfo(spark,jdbcReader):
    if "--fetchPartitionCol" in parms :
        mn,mx=None,None
        if "--lowerBound" in parms:
            mn =  int(parms["--lowerBound"])
            log.info("Using the provided lowerBound: {0}".format(mn))
        if "--upperBound" in parms:
            mx =  int(parms["--upperBound"])
            log.info("Using the provided upperBound: {0}".format(mx))

        if mn == None and mx == None :
            (mn,mx) = getMinMax(spark)
            log.info("Using the derieved --lowerBound {0} --upperBound {1}".format(mn,mx))
        elif mn == None:
            mn = getMinOrMax(spark=spark,typ='min')
            log.info("Using the derieved --lowerBound {0}".format(mn))
        elif mx == None:
            mx = getMinOrMax(spark=spark,typ='max')
            log.info("Using the derieved --upperBound {0}".format(mx))

    if ( mn == None and mx == None) :
        log.info("Not able to derieve the lower and upper bounds. Falling back to single threaded JDBC read.")
        return jdbcReader
    else:
        return jdbcReader.option("partitionColumn" ,parms["--fetchPartitionCol"]) \
                         .option("numPartitions"   ,parms["--jdbcReadSplits"])\
                         .option("lowerBound"      ,str(mn))\
                         .option("upperBound"      ,str(mx))

def getDistinctWritePartionColValues(df,partitionColList):
    split  = list(map(lambda c: c.strip().upper(),  partitionColList.split(",")))
    fields = list(filter(lambda f: (f.name.upper() in split)  , df.schema.fields ))

    log.debug ("splits: {}".format(split))
    log.debug ("fields: {}".format(str(fields)))

    #https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-CreatePartition
    def getFltr(split):
        res = None
        for field in fields:
            log.debug("split: {} , filed {} of type {}".format(split, field, str(field.dataType)))
            if field.name.upper() == split.upper():
                log.debug("field name {} match with {}".format(field.name, split))
                if type(field.dataType) in [StringType,DateType,TimestampType]:
                    res = "'{0}=' , '{0}'".format(field.name.upper())
                    log.info("inside String type")
                elif type(field.dataType) in [IntegerType, LongType, ShortType, DecimalType,DoubleType,FloatType]:
                    res = "'{0}=' , {0}".format(field.name.upper())
                    log.info("inside Numeric type")
                else:
                    log.info("filed's type didn't match :( {}".format(type(field.dataType)))
            else :
                log.debug("field name {} did not match with {}".format(field.name, split))
            if res != None:
                break
        log.debug("Final Result  {}".format(res))
        return res

    pFields= list(map(getFltr, split))
    columns = "concat( "+ " , ',', ".join( pFields ) + " ) as PARTITIONS"

    log.debug ("pFields: {}".format(str(pFields)))
    log.debug("concat stmt: {0}".format(columns))

    return df.selectExpr(columns).distinct()

def dropPartitions(spark, df, partitionColList, tablePath, deleteFiles=False):
    log.info("Deleting the partitions (if exists) under the s3 Path : {} ".format (tablePath))
    dfCols = getDistinctWritePartionColValues( df, partitionColList)

    for x in dfCols.collect():
        (bkt,key) = getBucketNKeyTuple( "{0}/{1}".format(tablePath,x['PARTITIONS'].replace("'","")))
        if deleteFiles:
            deleteS3Object (bkt,key)
        sql = "ALTER TABLE {0}.{1} DROP IF EXISTS PARTITION ({2})" \
              .format( parms["--dstDB"],parms["--dstTable"], x['PARTITIONS'])
        log.info("Altering the table by running the SQL,{}".format(sql))
        spark.sql(sql)
        spark.sql("refresh table {0}.{1}".format( parms["--dstDB"],parms["--dstTable"], x['PARTITIONS']))

def getNewPartitionFolderCount(srcDF, partitionColList):
    df = getDistinctWritePartionColValues(srcDF, partitionColList)
    return df.count()

def repartitionIfNeeded(df):
    curPartitionCnt  = df.rdd.getNumPartitions()
    repartitionCount = int(parms.get("--writeFileCount", parms.get("--jdbcReadSplits", str(curPartitionCnt) ) ))

    if "--writePartitionCol" in parms:
        div = 1 if "-ignoreOutPart" in parms \
              else \
                 getNewPartitionFolderCount(df, parms["--writePartitionCol"])

        repartitionCount =  int( repartitionCount /  div )

    log.info("Number RDDs/File to be produced : {}".format(repartitionCount))

    if repartitionCount == 1 or repartitionCount < curPartitionCnt:
        return df.coalesce(repartitionCount)
    elif repartitionCount == curPartitionCnt:
        return df.repartition(repartitionCount)
    else:
        return df

def createOrReplaceTable(spark,dfName,tableLocation):
    (bkt,key) = getBucketNKeyTuple(tableLocation)
    deleteS3Object(bkt,key)

    spark.sql("drop table if exists {0}.{1}".format(parms["--dstDB"],parms["--dstTable"]))
    partitionBy = ''
    if "--writePartitionCol" in parms:
        partitionBy  = "partitioned by ( {} ) ".format(parms["--writePartitionCol"])
        '''

        ddlSql = """
        create table {0}.{1} using {2} {3}
            location '{4}'
        TBLPROPERTIES ("auto.purge"="true")
                as select * from {5}
        """.format(parms["--dstDB"],
                   parms["--dstTable"],
                   parms["--outFlFmt"],
                   partitionBy,
                   tableLocation,
                   dfName)
        '''
        ddlSql = """
        create table {0}.{1} using {2} {3}
        TBLPROPERTIES ("auto.purge"="true")
                as select * from {4}
        """.format(parms["--dstDB"],
                   parms["--dstTable"],
                   parms["--outFlFmt"],
                   partitionBy,
                   dfName)


        logln = "Creating the table {0}.{1} using the DDL,\n{2}" \
                .format(parms["--dstDB"],parms["--dstTable"],ddlSql)
        log.info(logln)
        spark.sql(ddlSql)

def insertIntoTable(spark,df,dfName,tablePath):
    insertType = "insert into"
    partition =   "partition ( {} )".format(parms["--writePartitionCol"]) \
               if "--writePartitionCol" in parms else " "

    if parms["--writeMode"] == 'dropPartFirst':
        dropPartitions(spark, df, parms["--writePartitionCol"], tablePath, True)
    elif parms["--writeMode"] == 'insertOverwrite':
        insertType = "insert overwrite table "

    insertSQL = """
    {0} {1}.{2} {3}
    select * from {4}
    """.format(insertType,
                parms["--dstDB"],
                parms["--dstTable"],
                partition,
                dfName)
    logLn = "Inserting records into the table {0}.{1} using the SQL,{2}"\
            .format(parms["--dstDB"],parms["--dstTable"],insertSQL)
    log.info(logLn)
    spark.sql(insertSQL)

def start(argv):
    '''
    Driver function for handling the ingest
    '''
    argsToDict(argv)
    setupLogger()
    validateArgs()
    handleSSM()
    reportRunTimeParms()

    buidMainSQL()
    spark = generateSparkSession("Ingest-" +  parms["--dstDB"] + "." + parms["--dstTable"])

    inDF  = readFromRDBMS(spark)

    inDFName = "jdbcDf_" + parms["--dstDB"] + "_" + parms["--dstTable"]
    inDF.createOrReplaceTempView(inDFName)
    xFrmSQL = """
    select {0}.*
         {1}
        from {0}
    {2}
    """.format(inDFName,
               '' if "--newCols"           not in parms else ", " + parms["--newCols"] ,
               '' if "--writePartitionCol" not in parms else "    DISTRIBUTE BY {} ".format(parms["--writePartitionCol"])
                )
    log.info("SQL statement for building transformed DF\n{}".format(xFrmSQL))
    newDF =  spark.sql(xFrmSQL)
    newDFName = "DF_" + parms["--dstDB"] + "_" + parms["--dstTable"]
    newDF.createOrReplaceTempView(newDFName)
    log.info("Registered the dataframe as {}".format(newDFName))

    newDF.printSchema()
    if "-showRecs" in parms or parms["--logLevel"] == 'd':
        newDF.show(5)

    tablePath = ''
    tableMeta = getTableMeataDatFromGlue ( parms["--dstDB"] , parms["--dstTable"])
    if tableMeta == None :
        if parms["--writeMode"] != 'overwrite':
            log.info("Not able to find Table Metadata from Glue. Overriding --writeMode as 'overwrite'")
            parms["--writeMode"] = 'overwrite'
        dbPath     =      parms["--dbPath"]  if "--dbPath" in parms \
                     else getFromSSM("/datalake/glue/%s" % ( parms["--dstDB"].upper()))
        tablePath = "{0}/{1}".format(dbPath,parms["--dstTable"] )
    else :
        tablePath = tableMeta['StorageDescriptor']['Location']

    log.info("Table path : " + tablePath )
    if parms["--writeMode"] == 'overwrite':
        createOrReplaceTable(spark,newDFName,tablePath)
    else :
        insertIntoTable(spark,newDF,newDFName,tablePath)

    inDF.unpersist()
    newDF.unpersist()

    spark.stop()

if __name__ == "__main__":
    start(sys.argv)
