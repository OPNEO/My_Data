from kafka.admin import KafkaAdminClient
admin=KafkaAdminClient(bootstrap_servers=['localhost:9092'])
topics=admin.list_topics()
for x in topics:
    print(x)
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/dvdrental") \
    .option("dbtable", "customer") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()
output_path = "C:/ApacheSpark/files/customer_csv"

# Write the DataFrame to CSV
# Use mode("overwrite") if the directory might already exist and you want to replace it
df.write.format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(output_path)