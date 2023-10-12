from datetime import datetime
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains, when, col, sum, desc

# try:
#     print("Using Spacy...")
#     from spacy.lang.en import stop_words as stop_words
#     stopwords = list(stop_words.STOP_WORDS)
#     print("SpacyStopwords: ", stopwords)
# except ModuleNotFoundError:
#     print("Spacy error. Using NLTK... ")
#     import nltk
#     from nltk import corpus
#     nltk.download('stopwords')
#     stopwords = corpus.stopwords.words("english")
#     print("NLTKStopWords: ", stopwords)

# except AttributeError:
stopwords = [
    'before', '‘d', 'other', 'whence', 'itself', 'nobody', 'about', 'bottom',
    'always', 'was', 'us', 'my', 'further', 'a', 'these', 'forty', 'yet', 'where', 'whoever', 
    'are', 'sixty', 'been', 'everything', 'somehow', 'get', 'and',  'just', 'hereupon', 'various',
    'unless', 'own', 'seeming', '‘m', 'from', 'became', 'none', 'after', 'via', 'herself', 
    "'re", 'whatever', 'one', 'less', 'above', 'too', 'least', 'below', 'anything', 'well', 'anyhow',
    'few', 'front', 'thereafter', 'hers', 'of', 'any', 'another', 'anyway', 'already', 'why', 'now', 
    'yourself', "'ve", 'would', 'therefore', 'alone', 'whereas', 'but', 'meanwhile', 'between', 
    'wherever', 'in', 'much', 'all', 'within', 'himself', 'had', 'throughout', 'this', 'under', 'seems', 
    'ten', 'around', 'did', 'while', 'toward', 'when', 'more', 'everyone', 'seemed', 'serious', 
    'still', 'nine', 'whether', 'for', 'move', 'most', "n't", 'i', 'must', 'only', 'some', 'what', 'you', 
    'except', 'which', 'perhaps', 'side', '’re', 'without', 'thru', 'afterwards', 'eleven', 'else', 'your',
    'both', 'made', 'noone', 'through', 'becomes', 'upon', 'though', 'ever', 'we', 'against', 'towards', 
    'former', 'amongst', 'name', 'thus', 'he', 'out', 'thereupon', 'could', 'indeed', 'it', 'wherein', 
    'until', 'off', 'something', 'with', 'say', 'am', 'third', 'thereby', 'really', 'top', '’s', 
    'may', 'neither', 'however', 'used', 'whenever', 'two', 'per', 'its', 'seem', 'the', 'whom', 'also', 
    'so', 'sometimes', 'anywhere', 'is', 'beforehand', 'nor', '‘re', 'over', 'last', 'nevertheless', 'very', 
    'because', 'part', 'become', 'please', 'thence', 'hence', 'that', 'as', 'who', 'beyond', 'me', 'being',
    'quite', 'others', '’ll', 'empty', 'to', 'besides', 'anyone', 'whereafter', 'done', 'at', 'whose', 'be',
    'keep', 'than', 'they', 'three', 'n‘t', 'mostly', 'eight', 'fifteen', 'regarding', 'him', 'or', 
    'nothing', 'even', 'on', 'nowhere', 'yours', 'back', 'full', 'not', 'herein', 'mine', 'into', 'almost', 
    'during', 'take', 'moreover', 'how', 'by', '‘ve', 'them', 'hundred', 'down', 'go', 're', 'since', 
    'twenty', 'latter', 'their', 'five', 'up', 'his', 'hereby', 'those', 'ca', 'yourselves', 'again', 'among', 
    'onto', 'enough', 'can', 'were', 'put', 'otherwise', 'make', '‘ll', 'first', 'many', "'d", 'someone', 
    'doing', 'our', 'she', 'see', 'n’t', 'rather', 'either', 'ourselves', 'might', 'whole', 'therein', 
    'does', 'should', 'themselves', 'myself', 'hereafter', 'fifty', 'never', 'there', 'next', 'every', 'if',
    'show', 'here', "'ll", '’m', 'do', 'elsewhere', 'six', '‘s', 'amount', 'ours', '’ve', 'will', "'s", 
    'namely', 'once', 'same', 'such', 'often', 'have', 'using', 'twelve', "'m", 'behind', 'her', 'each', 
    'somewhere', '’d', 'due', 'everywhere', 'cannot', 'has', 'formerly', 'call', 'give', 'sometime', 
    'whither', 'across', 'together', 'becoming', 'an', 'several', 'whereby', 'no', 'along', 'latterly', 
    'although', 'beside', 'four', 'whereupon', 'then'
]


bucket = 'useranalytics-pipeline-bucket'
staging_bucket = 'useranalytics-pipeline-bucket-staging'
key = 'data/movie_review.csv'

spark = SparkSession.builder.appName("ProcessMovieReview").getOrCreate()

df = spark.read.csv(f'gs://{bucket}/{key}', inferSchema=True, header=True, sep=',')

tokenizer = Tokenizer(inputCol="review_str", outputCol="tokens")
stopwords_remover = StopWordsRemover(inputCol="tokens", outputCol="words", stopWords=stopwords)

pipeline = Pipeline(stages=[tokenizer, stopwords_remover])
result = pipeline.fit(df).transform(df)

result = result.withColumn("positive_review", array_contains(result["words"], "good"))

result = result.withColumn("positive_review", when(col("positive_review") == True, 1).otherwise(0))

result = result.selectExpr("cid AS user_id", "positive_review", "id_review AS review_id")
result.show()

date = datetime.now().strftime("%Y/%m/%d")
result.write.parquet(f'gs://{staging_bucket}/movie_review/{date}', mode="overwrite")

