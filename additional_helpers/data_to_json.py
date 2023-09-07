import configparser
import os
import re
import json
import boto3
from itertools import chain
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (split, lit,
                                   monotonically_increasing_id,
                                   create_map, coalesce)


class SaveToJson:
    """ Generates the json data files """

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('../../aws.cfg')
        self.aws_id = config['AWS']['AWS_ACCESS_KEY_ID']
        self.aws_secret_key = config['AWS']['AWS_SECRET_ACCESS_KEY']
        self.aws_default_region = config['AWS']['DEFAULT_REGION']
        self.input_data = config['DATA']['INPUT_DATA_PATH']
        self.output_data = config['DATA']['OUTPUT_DATA_PATH']
        self.bucket_name = config['DATA']['BUCKET_NAME']
        self.local_output = config['DATA']['LOCAL_OUTPUT']
        self.local_output_data_path = config['DATA']['LOCAL_OUTPUT_DATA_PATH']

    def create_spark_session(self):
        conf = SparkConf()
        conf.set(
            "spark.jars.packages",
            "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")

        """Create a apache spark session."""
        spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .enableHiveSupport() \
            .getOrCreate()
        print('Spark Session 생성 성공')
        return spark

    def create_s3_bucket(self, bucket_name):
        s3_resource = boto3.client(
            's3',
            region_name=self.aws_default_region,
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_secret_key
        )
        print('s3 resource 설정')

        buckets = s3_resource.list_buckets()

        for bucket in buckets['Buckets']:
            if bucket_name == bucket['Name']:
                s3_resource.delete_bucket(Bucket=bucket['Name'])

        s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

        print('s3 bucket 생성')

    def set_state_regex(self):
        state_dict = {}
        f = open('{}/label_descriptions/i94_adr.txt'.format(self.input_data), 'r')
        for line in f.readlines():
            stripped_string = re.sub('[^A-Za-z0-9=]+', ' ', line)
            state_code, state_name = stripped_string.strip().split("=")
            state_dict[state_code.strip()] = state_name.strip()

        return state_dict.keys()

    def set_label_dict(self):
        city_dict = {}
        f = open('{}/label_descriptions/i94_port.txt'.format(self.input_data), 'r')
        for line in f.readlines():
            stripped_string = re.sub('[^A-Za-z0-9=]+', ' ', line)
            city_code, city_name = stripped_string.strip().split("=")
            city_name_list = city_name.strip().split()
            city_name_only = ' '.join(
                [i for i in city_name_list if i not in self.set_state_regex()])
            city_dict[city_code.strip()] = city_name_only.strip()
        
        city_json = json.dumps(city_dict)
        f = open('{}/label_descriptions/city_codes.json'.format(self.input_data), 'w')
        f.write(city_json)
        f.close()
        return city_dict

    def process_immigration_data(self, spark, input_data, output_data):
        # Read and load the immigration data from the SAS dataset
        # to a spark dataframe
        print('immigration 데이터 읽는 중')

        dir_list = os.listdir(input_data +
                              'immigration_data/')

        for sas_file in dir_list:
            immigration_data = input_data + \
                'immigration_data/' + sas_file
            print('reading data from '+immigration_data+'...')
            immigration_df = spark.read.format(
                'com.github.saurfang.sas.spark').load(immigration_data)

        print('Immigration data를 df에 load')

        city_dict = self.set_label_dict()
        immigration_df = immigration_df.filter(
            immigration_df.i94port.isin(
                list(city_dict.keys())))
        
        mapping_expr = create_map([lit(x) for x in chain(*city_dict.items())])
        
        # extract columns for the immigration table
        immigration_table = immigration_df.select(
            'cicid',
            'i94yr', 'i94mon',
            'i94cit', 'i94res',
            'i94port', 'arrdate',
            'i94addr', 'depdate',
            'i94bir', 'i94visa',
            'gender', 'airline',
            'visatype') \
            .withColumnRenamed('cicid', 'immigration_id') \
            .withColumnRenamed('i94yr', 'year') \
            .withColumnRenamed('i94mon', 'month') \
            .withColumnRenamed('i94res', 'country') \
            .withColumnRenamed('arrdate', 'arrival_date') \
            .withColumnRenamed('i94addr', 'state') \
            .withColumnRenamed('depdate', 'departure_date') \
            .withColumnRenamed('i94bir', 'age') \
            .withColumnRenamed('i94visa', 'visa_code') \
            .withColumnRenamed('visatype', 'visa_type') \
            .withColumn('city', coalesce(mapping_expr[immigration_df['i94port']])) \
            .withColumn('visitor_id',
                        monotonically_increasing_id()) \
            .dropDuplicates()

        print('Immigration data Column 추출 성공')
        immigration_table.show()
        # write immigration table to json files
        immigration_table.write.json(os.path.join(output_data, 'immigration/'),
                                     'overwrite')

        print('immigration table을 json으로 변환 성공')

    def process_temperature_data(self, spark, input_data, output_data):
        # Read and load the temperature data from csv
        # to a spark dataframe
        print('temperature 데이터 읽는 중')

        temperature_data = input_data + \
            'temperature_data/GlobalLandTemperaturesByCity.csv'
        print('path to temperature data', temperature_data)
        temperature_df = spark.read.load(
            temperature_data,
            format="csv",
            header="true"
        )

        print('temperature data를 df에 load')

        temperature_df = temperature_df.filter(temperature_df.dt >= lit("2010-01-01"))

        # extract columns for the temperature table
        temperature_table = temperature_df.select(
            'dt',
            'AverageTemperature',
            'City',
            'Latitude',
            'Longitude') \
            .withColumnRenamed('dt', 'date') \
            .withColumnRenamed('AverageTemperature',
                               'average_temperature') \
            .withColumnRenamed('City', 'city') \
            .withColumnRenamed('Latitude', 'latitude') \
            .withColumnRenamed('Longitude', 'longitude') \
            .dropDuplicates()

        print('temperature data Column 추출 성공')

        temperature_table.write.json(
            os.path.join(
                output_data,
                'temperature/'),
            'overwrite')

        print('temperature table을 json으로 변환 성공')

    def process_airport_data(self, spark, input_data, output_data):
        # Read and load the airport data from csv
        # to a spark dataframe
        print('airport 데이터 읽는 중')

        airport_data = input_data + 'airport-codes.csv'

        df = spark.read.load(
            airport_data,
            format="csv",
            header="true"
        )

        airport_table = df.select('ident', 'type',
                                  'name', 'continent',
                                  'iso_country', 'iso_region') \
            .withColumnRenamed('ident', 'airport_code') \
            .withColumnRenamed('iso_country', 'country_code') \
            .withColumn('region', split('iso_region', '-')[1]) \
            .dropDuplicates()

        print('airport data를 df에 load')

        airport_table.write.json(
            os.path.join(
                output_data,
                'airport/'),
            'overwrite')

        print('airport table을 json으로 변환 성공')

    def process_demographics_data(self, spark, input_data, output_data):
        # Read and load the demographics data from csv
        # to a spark dataframe
        print('demographics 데이터 읽는 중')

        demographics_data = input_data + 'us-cities-demographics.csv'

        df = spark.read.load(
            demographics_data,
            sep=';',
            format="csv",
            header="true"
        )

        demographics_table = df.select('City', 'State',
                                       'Male Population',
                                       'Female Population',
                                       'Total Population') \
            .withColumnRenamed('City', 'city') \
            .withColumnRenamed('State', 'state') \
            .withColumnRenamed('Male Population',
                               'male_population') \
            .withColumnRenamed('Female Population',
                               'female_population') \
            .withColumnRenamed('Total Population',
                               'total_population') \
            .dropDuplicates()

        print('demographics data를 df에 load')

        demographics_table.write.json(
            os.path.join(
                output_data,
                'demographics/'),
            'overwrite')

        print('demographics table을 json으로 변환 성공')

    def execute(self):
        if self.local_output:
            self.output_data = self.local_output_data_path
        else:
            self.create_s3_bucket(self.bucket_name)

        spark = self.create_spark_session()

        method_arguments = (spark, self.input_data, self.output_data)
        # self.process_airport_data(*method_arguments)
        # self.process_demographics_data(*method_arguments)
        self.process_temperature_data(*method_arguments)
        # self.process_immigration_data(*method_arguments)


# save_to_parquet = SaveToJson()
# save_to_parquet.execute()
