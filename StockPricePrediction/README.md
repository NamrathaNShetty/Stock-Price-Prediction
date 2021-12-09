#Stock Data Prediction Using Live Data
PROJECT SIMULATION ON STOCK PRICE PREDICTION USING LIVE DATA
Prerequisites:-
Hadoop 3.3.1
Spark 3.1.2
MLlib
AWS EC2
AWS S3
Python 3.8
Java

Library Used:
Alpha_Vantage Api: for getting stock data.
Flask: for rendering visualization of data in web browser


Step 1:
* Using ALPHA VANTAGE API to predict google stock price and to generate API Key.
* Using this API key to Download Google Stock Price data for each 1 min interval.
* Create S3 bucket using boto3. (Boto is the Amazon Web Services (AWS) SDK for Python. It enables Python developers to create, configure, and manage AWS services, such as EC2 and S3. Boto provides an easy to use, object-oriented API, as well as low-level access to AWS services.)
* After creating bucket upload stock data into bucket using boto3.

Step 2: 
* Reading data from S3 and doing some preprocessing.
* Cleaning the data by removing the col names 0,1,2,3,4,5
* Sorting the data and changing the datatypes of the columns.
* Check for the null values

Step 3: 
CREATING A ML MODEL
* Import necessary libraries
* Create features columns with vector assembler
* Trasform the data set accordingly
* Sort data in ascending order
* Split data into test and train data with window function
* Write test data to parquet file for further use
* Create model with linear regression algoritham
* Check for coefficient and intercepts
* Make prediction by transforming data in model
* Save model to HDFS

Step 4:
Create Flask API

In flask I have created an URL:

"/" for main page which rander index.html template for showing predicting close and actual close value.
"/data" this is for send data to index.html page for showing graph.
How To Run
Run app file. (python3 app.py)

Step 4:

Deploying stock prediction model on EC2 instance
* Create EC2 t2.small instance
* Now add another security group to EC2 instance and give all traffic permission to inbound rule.
* Connect to EC2 instance using FileZilla and Upload all the project files and kafka to ec2 instance.
* From terminal connect to EC2 instance using ssh command
* Update apt and python3-pip using this command
sudo apt-get update && sudo apt-get install python3-pip
* Install all the requirements using requirements.txt
pip3 install -r requirements.txt
* Install java jvm 8 in ec2 instance
sudo apt-get install default.jre
Run app file. (python3 app.py)


