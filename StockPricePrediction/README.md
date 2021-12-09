# Stock Data Prediction Using Live Data
PROJECT SIMULATION ON STOCK PRICE PREDICTION USING LIVE DATA
Prerequisites:-
Hadoop 3.3.1
Spark 3.1.2
MLlib
AWS EC2
AWS S3
Python 3.8

Library Used:
Alpha_Vantage Api: for getting stock data.
Flask: for rendering visualization of data in web browser

Step 1:
* Using ALPHA VANTAGE API to predict google stock price.
* Using ALPHA VANTAGE to genrate API Key.
* Using this API key to Download Google Stock Price data for each 1 min interval.
* Create S3 bucket using boto3. (Boto is the Amazon Web Services (AWS) SDK for Python. It enables Python developers to create, configure, and manage AWS services, such as EC2 and S3. Boto provides an easy to use, object-oriented API, as well as low-level access to AWS services.)
* After creating bucket upload stock data into bucket using boto3.

Step 2:
* Reading data from S3 and doing some preprocessing.
* After preprocessing train a Linear Regression model and save the model

Preprocessing the data to clean it and Creating ML model
1. Create a notebook and load create a spark session
2. Load the data we fetched in spark dataframe
3. Check the dataframe columns
4. Clean the column as they are not in expected format for analysis
5. Check the final cleaned data
6. Convert data types of columns to desired format
7. Check for null values by converting spark df to pandas df

CREATING A ML MODEL
1. Import necessary things
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
2. Create features columns with vector assembler
featureassembler=VectorAssembler(inputCols=["open","high","low"],outputCol="Features")
3. Trasform the data set accordingly
output=featureassembler.transform(df2)
4. Sort data in ascending order
finalized_data=output.select("time","Features","close").sort("time",ascending=True)
5. Split data into test and train data with window function
final_data=finalized_data.withColumn("rank",percent_rank().over(Window.partitionBy().orderBy("time")))
train_data = final_data.where("rank <= .8").drop("rank")
test_data = final_data.where("rank > .8").drop("rank")
6. Write test data to parquet file for further use
test_df.write.parquet('test_data')
7. Create model with linear regression algoritham
regressor=LinearRegression(featuresCol='Features', labelCol='close')
lr_model=regressor.fit(train_data)
8.Check for coefficient and intercepts
lr_model.coefficients
lr_model.intercept
9. Make prediction by transforming data in model
pred= lr_model.transform(test_data)
pred.select("Features","close","prediction").show()
Output:
Features	close	prediction
[139.045,139.085,...	138.98	139.0088189816052
[138.97,138.98,13...	138.97	138.98604284027746
[139.21,139.21,13...	139.21	139.218743620076
[138.81,138.81,13...	138.81	138.81953685610827
[138.81,139.05,13...	139.05	138.98329554207902
[139.4355,139.435...	138.9	139.03487193320166
[139.25,139.25,13...	139.25	139.25866429647274
[138.97,138.97,13...	138.97	138.97921956169534
[139.43,139.59,13...	139.59	139.53984348571282
10. Save model to HDFS
lr_model.save(“Stock_Model”)

Create Flask API:

In flask I have created 2 URL:

"/" for main page which rander index.html template for showing predicting close and actual close value.
"/data" this is for send data to index.html page for showing graph.
How To Run
Run app file. (python3 app.py)

Deploying stock prediction model on EC2 instance:

1. Create EC2 t2.small instance
2. Now add another security group to EC2 instance and give all traffic permission to inbound rule.
3. Connect to EC2 instance using FileZilla and Upload all the project files to ec2 instance.
4. From terminal connect to EC2 instance using ssh command
ssh -i Namratha_EC2.pem ubuntu@ec2-3-108-249-9.ap-south-1.compute.amazonaws.com
5. Update apt and python3-pip using this command
sudo apt-get update && sudo apt-get install python3-pip
6. Install all the requirements using requirements.txt
pip3 install -r requirements.txt
7. Install java jre in ec2 instance
sudo apt-get install default.jre
8. Run app file. (python3 app.py)
