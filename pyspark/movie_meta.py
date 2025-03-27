from pyspark.sql import SparkSession, DataFrame
import sys
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

spark = SparkSession.builder.appName("movie_meta").getOrCreate()

exit_code = 0 # O 이면 정상 종료, 1 이면 비정상 종료
BACK_UP_PATH = "/Users/joon/swcamp4/data/movie_spark/backup"

def save_meta(df: DataFrame, meta_path: str):
    df.write.mode("overwrite").parquet(meta_path)
    logging.info("meta 저장 완료", meta_path)

def get_update_meta_sql() -> str:
    return """
        SELECT
            movieCd,
            COALESCE(MAX(multiMovieYn)) AS multiMovieYn,
            COALESCE(MAX(repNationCd)) AS repNationCd
        FROM (
            SELECT movieCd, multiMovieYn, repNationCd FROM temp_meta_yesterday
            UNION ALL
            SELECT movieCd, multiMovieYn, repNationCd FROM temp_meta_today
        ) merged
        GROUP BY movieCd
    """

def get_assert_meta_sql() -> str:
    return """
        SELECT ASSERT_TRUE(
            (SELECT COUNT(*) FROM temp_meta_update) > (SELECT COUNT(*) FROM temp_meta_yesterday) AND
            (SELECT COUNT(*) FROM temp_meta_update) > (SELECT COUNT(*) FROM temp_meta_today)
        ) AS is_valid
    """

try:    
    if len(sys.argv) != 4:
        raise ValueError("필수 인자가 누락되었습니다")
    
    raw_path, mode, meta_path = sys.argv[1:4]
    
    meta_today = spark.read.parquet(raw_path).select("movieCd", "multiMovieYn", "repNationCd")
    
    if mode == "create":
        save_meta(meta_today, meta_path)
    elif mode == "append":
        meta_yesterday = spark.read.parquet(meta_path)
        meta_yesterday.createOrReplaceTempView("temp_meta_yesterday")
        meta_today.createOrReplaceTempView("temp_meta_today")
        save_meta(meta_yesterday, BACK_UP_PATH) # 이전 meta 백업
        
        update_sql = get_update_meta_sql()
        updated_meta = spark.sql(update_sql)
        updated_meta.createOrReplaceTempView("temp_meta_update")
        
        assert_sql = get_assert_meta_sql()
        spark.sql(assert_sql)
        
        save_meta(updated_meta, meta_path)
    else:
        raise ValueError(f"알 수 없는 MODE: {mode}")
except Exception as e:
    logging.error(f"오류 : {str(e)}")
    exit_code = 1
finally:
    spark.stop()
    sys.exit(exit_code)