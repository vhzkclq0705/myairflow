from pyspark.sql import SparkSession
import sys
import logging

spark = SparkSession.builder.appName("movie_meta").getOrCreate()
logging.basicConfig(level=logging.INFO)

exit_code = 0 # O 이면 정상 종료, 1 이면 비정상 종료

try:    
    if len(sys.argv) != 4:
        raise ValueError("필수 인자가 누락되었습니다")
    
    raw_path, mode, meta_path = sys.argv[1:4]
    
    raw_df = spark.read.parquet(raw_path)
    raw_df.show()
    raw_df.select("movieCd", "multiMovieYn", "repNationCd").show()
    
    print("*" * 33)
    print("DONE")
    logging.debug("DEBUG~")
    logging.info("INFO~")
    logging.warning("WARNING~")
    logging.error("ERROR~")
    logging.critical("CRITICAL~")
    print("*" * 33)
        
except Exception as e:
    logging.error(f"오류 : {str(e)}")
    exit_code = 1
finally:
    spark.stop()
    sys.exit(exit_code)