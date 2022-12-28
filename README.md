# Python-ETL-Send-SQLServer-Tables-to-S3-Bucket-in-Parquet-format

This Python ETL gets the data, transforms a SQL Server table or querry into Parquet format file and sends it to an S3 Bucket

Pre-requisites:

    - Install python and the required modules
    - An SQL Server instance and a S3 Bucket instance


How to run:

    1. Chnage the SQL Server database and S3 Bucket credentials in ´ETL.py´
    2. run ´py .\ETL.py´ on the project directory


Suggested way to compile:

    I suggest compiling this way. Since it allows us to run it as a service:

    ´pyinstaller --clean -y -n "ETL_Assinaturas" --add-data="querries.json;files" .\ETL.py´

    Make sure all the required folders and files are present after compiling.
