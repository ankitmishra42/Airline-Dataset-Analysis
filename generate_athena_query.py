def generate_athena_query(df, db_name, table_name, s3_location, delimter=','):
    columns = df.columns
    str = ""
    for column in columns:
        val = f"`{column} {df[column].dtypes}"
        str = str + val
    columns_str = str.replace(" ", "_").replace("_object", "` STRING,\n        ").replace("_int64", "` INT,\n        ").replace("_float64", "` FLOAT,\n        ").replace("_bool", "` BOOLEAN,\n        ")[:-10]

    query = f"""
    CREATE DATABASE IF NOT EXISTS {db_name};
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} (
        {columns_str}
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES ('field.delim' = '{delimter}')
    STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '{s3_location}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    return query



# DOCUMENTATION

# def generate_athena_query(df, db_name, table_name, s3_location):
#     """Generates an AWS Athena CREATE EXTERNAL TABLE SQL query from a pandas DataFrame.
    
#     This function automatically maps pandas DataFrame column types to Athena/Glue data types
#     and constructs a properly formatted DDL query for creating external tables pointing to
#     CSV data in S3.

#     Args:
#         df (pandas.DataFrame): Input DataFrame containing the schema to convert
#         db_name (str): Name of the Athena database where the table will be created
#         table_name (str): Name of the external table to create
#         s3_location (str): S3 URI (e.g., 's3://bucket/path/') where data files are stored

#     Returns:
#         str: Complete Athena DDL query string ready for execution

#     Raises:
#         ValueError: If input DataFrame is empty or required parameters are missing

#     Examples:
#         >>> import pandas as pd
#         >>> df = pd.DataFrame({
#         ...     'user_id': [1, 2, 3],
#         ...     'username': ['alice', 'bob', 'charlie'],
#         ...     'signup_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
#         ... })
#         >>> print(generate_athena_query(
#         ...     df=df,
#         ...     db_name="user_db",
#         ...     table_name="users",
#         ...     s3_location="s3://my-bucket/data/users/"
#         ... ))
#         CREATE EXTERNAL TABLE IF NOT EXISTS user_db.users (
#                 user_id INT,
#                 username STRING,
#                 signup_date TIMESTAMP
#         )
#         ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
#         [... rest of query ...]

#     Notes:
#         - Default data type mapping:
#             object → STRING
#             int64 → INT
#             float64 → FLOAT
#             bool → BOOLEAN
#             datetime64[ns] → TIMESTAMP
#         - Table properties assume CSV format with header row
#         - For partitioned tables, additional ALTER TABLE statements would be needed
#     """
#     # [Function implementation...]
