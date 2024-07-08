#import yfinance as yf
#import pandas as pd
from glob import glob
import os
from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession

# 주식 티커 심볼 리스트
#tickers = ['AAPL', 'GOOGL', 'BAC', 'XOM', 'JNJ', 'TSLA', 'AMZN']

# 데이터 다운로드 기간 설정
#start_date = '2013-01-01'
#end_date = '2023-12-31'

# 각 티커에 대해 데이터를 다운로드하고 CSV 파일로 저장
#for ticker in tickers:
    # 주식 데이터 다운로드
   # data = yf.download(ticker, start=start_date, end=end_date)
    
    # 데이터 확인
   # print(f"{ticker} 데이터:")
    #print(data.head())
    
    # CSV 파일로 저장
    #csv_filename = f"{ticker}_10_years.csv"
    #data.to_csv(csv_filename)
    #print(f"{ticker} 데이터를 {csv_filename} 파일로 저장했습니다.\n")

# 1. SparkConf 객체 생성 및 애플리케이션 이름 설정
spark_conf = SparkConf().setAppName("oversea_stock_simulation")

# 2. SparkContext 객체 생성 및 설정 적용
spark = SparkContext(conf = spark_conf)

# 3. SparkConf의 모든 설정 값 가져오기 (새 객체에 대해 수행)
# 3. 설정된 모든 설정 값 가져오기
all_conf = spark_conf.getAll()

print(all_conf)

spark = SparkSession.builder.master("spark://spark-master:7077").appName("oversea_stock_simulation").getOrCreate()

# CSV 파일이 있는 디렉토리
csv_directory = "/opt/bitnami/spark/data"

# 디렉토리 내의 모든 CSV 파일 찾기
csv_files = glob(os.path.join(csv_directory, "*.csv"))

# 각 CSV 파일에 대해 처리 수행
for csv_file in csv_files:
    print(f"Processing file: {csv_file}")
    
    # CSV 파일 읽기
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    
    # 스키마 출력
    df.printSchema()
    
    # DataFrame 출력
    df.show()

# # CSV 파일 읽기
# df = spark.read.csv(csv_file, header=True, inferSchema=True)

# # 스키마 출력
# df.printSchema()

# # DataFrame 출력
# df.show()

# Spark 세션 중지
spark.stop()