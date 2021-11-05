import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import when, to_date, col, date_format
from pyspark.sql.window import Window



def translate_language_Udf(column):
    """

    :param column:
    :return:
    """
    res_split = []
    reg_patterns = ["ficción|fiction/fiction","agosto|augu/august/", "settembre|septembre|septiembre|septiembrede |septiembrede|settembre/september/", "dicembre|décembre/december/", "ottobre|ottob/October", "marzo/March"]
    for i in range(len(reg_patterns)):
        res_split = re.findall(r"[^/]+", reg_patterns[i])
        for x in res_split[0].split("|"):
            column = column.replace(x, res_split[1])
    return column

def coauthor(df):
    """

    :param df:
    :return:
    """
    max_authors = df.withColumn('max_authers', F.size(F.col('authors'))).agg(F.max('max_authers')).first()[0]
    for num in range(max_authors)[1:]:
        df = df.withColumn('author{}'.format(num), F.when(F.col('authors')[num]['author']['key'].isNotNull(),
                                            F.col('authors')[num]['author']['key']).
                      when(F.col('authors')[num]['key'].isNotNull(), F.col('authors')[num]['key']).otherwise(''))

    return df

def genres_reconcile(df):
    """
    # the genre values are not as per expectations, there are some special char attached to them
    # Extra spaces are there,
    # duplicate entries in the genre's list
    # transform the values into lower case so that the cleaning of the data can be performed easily.
    :param df:
    :return:
    """
    df = df.withColumn('ngenres', F.explode(F.col('genres')))
    df = df.withColumn('ngenres', F.split(F.col("ngenres"), ",")).withColumn('ngenres', F.expr(
        """ array_distinct(transform(ngenres, x -> lower(trim(regexp_replace(x, "[.]", ''))))) """))
    df = df.withColumn('ngenres', F.explode(F.col('ngenres')))
    df = df.withColumn('ngenres', F.regexp_replace(F.col('ngenres'),r'\s*\d{4}\s*', ''))
    #df = df.filter(~F.col('ngenres').isin(['etc', ''])).dropDuplicates(['id', 'ngenres'])
    df = df.dropDuplicates(['id', 'ngenres'])
    return df

def title_extraction(df):
    """
    # remove those records where the titles are null
    # There are extra data attached to the title's values
    #   leading and trailing spaces
    #   special character
    #   camel_Case and lower-case title
    :param df:
    :return:
    """
    df = df.filter(df['title'].isNotNull())
    df=df.withColumn('ntitle', F.trim(df.title)).\
        withColumn('ntitle', F.regexp_replace(F.col('ntitle'), r'^\s*',''))\
        .withColumn('ntitle',F.translate(F.col('ntitle'),'!=-"\'\.',''))\
        .withColumn('ntitle', F.regexp_replace(F.col('ntitle'), r'^\s*', ''))
    return df

def author_reconcile(df):
    """
    root
         |-- authors: array (nullable = true)
         |    |-- element: struct (containsNull = true)
         |    |    |-- author: struct (nullable = true)
         |    |    |    |-- key: string (nullable = true)
         |    |    |-- key: string (nullable = true)
         |    |    |-- type: string (nullable = true)
         |-- sz: integer (nullable = false)

    :return:
    """
    # remove all the null and blank records
    df = df.filter('authors is not null')
    # Author information can be collected using author key, If there is no author key present
    # then we can check the key in the 'key' key of authors
    df = df.withColumn('mainAuthor', F.when(F.col('authors')[0]['author']['key'].isNotNull(),
                                            F.col('authors')[0]['author']['key']).otherwise(F.col('authors')[0]['key']))
    # co-authors list
    # ud = F.udf(lambda x: [y['key'] if y['key']!= None else y['author']['key'] for y in x], T.ArrayType(F.StringType()))
    # df = df.withColumn('co_authors', ud(F.col('authors')))
    # coauthor flag
    # df = df.withColumn('co_author_flag', F.when(F.size('co_authors') > 0, True).otherwise(False))
    df = coauthor(df)
    return df


def date_reconcile(df, year=False):
    """

    :param df:
    :return:
    """
    reg_replaceUdf = F.udf(translate_language_Udf, T.StringType())
    # Remove null and blank values
    df = df.filter('publish_date is not null')
    df = df.withColumn('publish_date', reg_replaceUdf(F.col('publish_date')))
    # covert all the values to lover case
    df = df.withColumn('publish_date', F.lower(F.col('publish_date')))
    # date parsing where we have th/nd/rd/st
    df = df.withColumn('publish_date', F.regexp_replace(col('publish_date'),
                                                        r'\s*(\d{1,2})th|nd|st|rd|\s*([a-z,A-Z]+)\s*(\d{4})', '$1$2 $3')
                       )
    # converting different date format to single format
    df = df.withColumn('publish_date1', when(to_date(col("publish_date"), "MMMM dd, yyyy").isNotNull(),
                                            date_format(to_date(col("publish_date"), "MMMM dd, yyyy"), "MM-dd-yyyy"))
                       .when(to_date(col("publish_date"), "MM/dd/yyyy").isNotNull(),
                             date_format(to_date(col("publish_date"), "MM/dd/yyyy"), "MM-dd-yyyy"))
                       .when(to_date(col("publish_date"), "yyyy/MM").isNotNull(),
                             date_format(to_date(col("publish_date"), "yyyy/MM"), "MM-dd-yyyy"))
                       .when(to_date(col("publish_date"), "MMMM, yyyy").isNotNull(),
                             date_format(to_date(col("publish_date"), "MMMM, yyyy"), "MM-dd-yyyy"))
                       .when(to_date(col("publish_date"), "MMMM d, yyyy").isNotNull(),
                             date_format(to_date(col("publish_date"), "MMMM d, yyyy"), "MM-dd-yyyy"))
                       .when(to_date(col("publish_date"), "dd MMM yyy").isNotNull(),
                             date_format(to_date(col("publish_date"), "dd MMM yyy"), "MM-dd-yyyy"))
                       .when(to_date(col("publish_date"), "MMM yyyy").isNotNull(),
                             date_format(to_date(col("publish_date"), "MMM yyyy"), "MM-dd-yyyy"))
                       .when(to_date(col("publish_date"), "yyyy MMMM dd").isNotNull(),
                             date_format(to_date(col("publish_date"), "yyyy MMMM dd"), "MM-dd-yyyy"))
                       .otherwise("Unknown Format"))
    if year:
        df = df.withColumn('publish_date1', when(to_date(col("publish_date"), "yyyy").isNotNull(),
                                                 date_format(to_date(col("publish_date"), "yyyy"), "MM-dd-yyyy")).otherwise(F.col('publish_date1')))
    df.filter('publish_date1 == "Unknown Format"').repartition(1).write.mode('overwrite').json("C:\case\discarded_records\\unknown_date.csv")
    df = df.filter('publish_date1 != "Unknown Format"')
    df = df.withColumn('publish_year', F.year(to_date(F.col('publish_date1'),'MM-dd-yyyy'))).withColumn('publish_month', F.month(to_date(F.col('publish_date1'),'MM-dd-yyyy')))
    df = df.filter('publish_year > 1950')
    return df


def profiling(df):
    print("Total number of records in the source data {}".format(df.count()))
    print("number of records after dropping duplicates {}".format(df.distinct().count()))
    print("Records where null/blank in title {}".format(df.filter('authors is null').count()))
    print("Records where null/blank in publish_date {}".format(df.filter('publish_date is null').count()))
    print("Records where null/blank in genres {}".format(df.filter('genres is null').count()))
    print("Records where null/blank in number_of_pages {}".format(df.filter('number_of_pages is null').count()))

def clean_data(df):
    """

    :param df:
    :return:
    """
    df = df.filter('authors is not null')
    df = df.filter('publish_date is not null')
    df = df.filter('number_of_pages is not null')
    print("Number of records after filtering null {}".format(df.count()))
    df = df.filter('number_of_pages > 20')
    print("Number of records after pages filter {} ".format(df.count()))
    df = author_reconcile(df)
    print("Number of records after author data cleansing {} ".format(df.count()))
    df = title_extraction(df)
    print("Number of records after title_extraction {} ".format(df.count()))
    df = date_reconcile(df, True)
    print("Number of records after date_reconcile {} ".format(df.count()))
    df = genres_reconcile(df)
    print("Number of records after genres_reconcile {} ".format(df.count()))


    return df

def queries(df):
    win1 = Window.partitionBy('mainAuthor').orderBy(F.col('publish_date1').asc())
    win2 = Window.partitionBy('mainAuthor').orderBy(F.col('publish_date1').desc())

    df1 = df.select('mainAuthor', 'publish_date1', 'ntitle').withColumn('rank', F.dense_rank().over(win1)).withColumn(
        'first_publish', F.when(F.col('rank') == 1, 1).otherwise(0)).drop('rank')
    df1 = df1.withColumn('rank', F.dense_rank().over(win2)).withColumn('last_publish',
                                                                       F.when(F.col('rank') == 1, 1).otherwise(0)).drop(
        'rank')
    print("Get the first book / author which was published -and the last one")
    df1.filter('first_publish=1 or last_publish=1').select('mainAuthor', 'ntitle', 'last_publish', 'first_publish').sort('mainAuthor').show(truncate=False)

    win3 = Window.orderBy(F.col('num_books').desc())
    df1 = df.select('ngenres', 'ntitle').groupBy('ngenres').agg(
        F.countDistinct('ntitle').alias('num_books')).withColumn('rank', F.row_number().over(win3)).filter(
        'rank<=5')
    print("Find the top 5 genres with most published books.")
    df1.show(truncate=False)

    print("Per publish year, get the number of authors that published at least one book")
    df.groupBy('publish_year').agg(F.countDistinct('mainAuthor').alias('num_authors')).show(truncate=False)
    print("Find the number of authors and number of books published per month for years between 1950 and 1970!")
    win4 = Window.partitionBy('publish_year', 'publish_month').orderBy(F.col('publish_month').asc())
    df1 = df.select('mainAuthor', 'ntitle', 'publish_month', 'publish_year').filter(
        'publish_year > 1950 and publish_year < 1970').withColumn('num_books', F.size(
        F.collect_set('ntitle').over(win4))).withColumn('num_author',
                                                        F.size(F.collect_set('mainAuthor').over(win4))).select(
        'publish_year', 'publish_month', 'num_author', 'num_books').distinct().sort(
        ['publish_year', 'publish_month'])
    df1.show()
if __name__ == '__main__':

    spark = spark = SparkSession.builder.appName("data_import").config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true").enableHiveSupport().getOrCreate()
    # read the raw data
    df = spark.read.json('C:\case\library.json')

    # basic data profiling
    profiling(df)
    df = df.withColumn('id', F.monotonically_increasing_id())
    df = df.select('id', 'title', 'authors', 'genres', 'publish_date', 'number_of_pages')
    # clean the data for further validation
    df = clean_data(df)
    # Write clean data
    #df.write.partitionBy('publish_year').mode('overwrite').parquet('C:\case\clean_data')
    queries(df)
    print('final count to run the queries {}'.format(df.count()))
    df.show(truncate=False)