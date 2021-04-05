// Databricks notebook source
import org.apache.spark.sql.functions._

val users_csv = sqlContext.read.format("com.databricks.spark.csv")
                .option("delimiter", "::")
                .load("/FileStore/tables/user-1.csv")
var column_names = Seq("user_id", "name", "url")
val users = users_csv.toDF(column_names:_*)

val businesses_csv = sqlContext.read.format("com.databricks.spark.csv")
                .option("delimiter", "::")
                .load("/FileStore/tables/business_1_-1.csv")
column_names = Seq("business_id", "address", "categories")
val businesses = businesses_csv.toDF(column_names:_*)

val reviews_csv = sqlContext.read.format("com.databricks.spark.csv")
                .option("delimiter", "::")
                .load("/FileStore/tables/review_1_-1.csv")
column_names = Seq("review_id", "user_id", "business_id", "stars")
val reviews = reviews_csv.toDF(column_names:_*)

users.createOrReplaceTempView("users")
businesses.createOrReplaceTempView("businesses")
reviews.createOrReplaceTempView("reviews")

print("Question 1\n")
var q1 = sqlContext.sql("SELECT business_id, COUNT(business_id), ROUND(AVG(stars), 2) FROM reviews GROUP BY business_id").show()
var q2 = businesses.withColumn("address", split($"address", ","))
                   .withColumn("address", $"address"(size($"address") - 1))
                   .withColumn("address", split($"address", " "))
                   .withColumn("state", $"address"(1))

q2.createOrReplaceTempView("q2")
print("Question 2\n")
var states = sqlContext.sql("SELECT q2.state, COUNT(q2.business_id), (ROUND(AVG(reviews.stars), 2)) FROM q2 INNER JOIN reviews ON reviews.business_id=q2.business_id GROUP BY q2.state ORDER BY ROUND(AVG(reviews.stars), 2) DESC").show()

var q3 = businesses.filter($"categories".contains("Colleges & Universities"))
q3.createOrReplaceTempView("q3")
print("Question 3\n")
var colleges = sqlContext.sql("SELECT users.user_id, users.name, (ROUND(AVG(stars), 2)) FROM q3 INNER JOIN reviews ON q3.business_id=reviews.business_id INNER JOIN users ON users.user_id=reviews.user_id GROUP BY users.user_id, users.name").show()

print("Question 4\n")
var NY = sqlContext.sql("SELECT businesses.business_id, businesses.address, businesses.categories, (ROUND(AVG(reviews.stars), 2)) FROM businesses INNER JOIN q2 ON businesses.business_id=q2.business_id INNER JOIN reviews on businesses.business_id=reviews.business_id WHERE q2.state = 'NY' GROUP BY businesses.business_id, businesses.address, businesses.categories ORDER BY (ROUND(AVG(stars), 2)) DESC LIMIT 10").show()

print("Question 5\n")
var multiple = sqlContext.sql("SELECT users.user_id, users.name, (ROUND(AVG(reviews.stars), 2)) FROM users INNER JOIN reviews ON users.user_id=reviews.user_id INNER JOIN q2 ON reviews.business_id=q2.business_id GROUP BY users.user_id, users.name HAVING (COUNT(DISTINCT(q2.state)) > 1)").show()



// COMMAND ----------


