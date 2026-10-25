# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Databricks AI Functions - Beginner Examples
# MAGIC AI Functions are built-in SQL functions that call a hosted language model for you.
# MAGIC No model setup, no ML code: you pass text in and get a result column back.
# MAGIC
# MAGIC | Function | What it does |
# MAGIC |---|---|
# MAGIC | `ai_analyze_sentiment(text)` | returns positive / negative / neutral / mixed |
# MAGIC | `ai_summarize(text, max_words)` | short summary of the text |
# MAGIC | `ai_classify(text, labels)` | picks the best label from a list you give |
# MAGIC | `ai_extract(text, fields)` | pulls named fields (e.g. product, city) out of text |
# MAGIC | `ai_translate(text, lang)` | translates text to a language code like `hi` |
# MAGIC | `ai_mask(text, fields)` | hides personal data such as names or phone numbers |
# MAGIC | `ai_query(endpoint, prompt)` | sends any prompt to a model endpoint (most flexible) |
# MAGIC
# MAGIC Note: availability and quotas depend on your plan and region. If a function errors on Free Edition, that function is likely not enabled there.

# COMMAND ----------

# DBTITLE 1,Sample data: customer reviews
from pyspark.sql.functions import expr

reviews = [
    (1, "The TV arrived late but the picture quality is amazing. Delivered to Noida."),
    (2, "Too expensive for what it offers, I would not buy again."),
    (3, "Fridge works great and the delivery was very quick. Call me on 9876543210."),
]
df = spark.createDataFrame(reviews, ["review_id", "review"])
df.createOrReplaceTempView("reviews")
df.display()

# COMMAND ----------

# DBTITLE 1,Example 1: Sentiment analysis
# ai_analyze_sentiment reads each review and returns a sentiment label.
# It runs row by row, so it works on a whole table the same way as on one value.
spark.sql("""
    SELECT review_id, review, ai_analyze_sentiment(review) AS sentiment
    FROM reviews
""").display()

# COMMAND ----------

# DBTITLE 1,Example 2: Summarize
# The second argument is the maximum number of words in the summary.
spark.sql("""
    SELECT review_id, ai_summarize(review, 8) AS short_summary
    FROM reviews
""").display()

# COMMAND ----------

# DBTITLE 1,Example 3: Classify into topics
# You choose the labels; the model picks the closest one for each review.
spark.sql("""
    SELECT review_id, review,
           ai_classify(review, ARRAY('delivery', 'quality', 'price')) AS topic
    FROM reviews
""").display()

# COMMAND ----------

# DBTITLE 1,Example 4: Mask personal data
# Useful before sharing data: phone numbers and similar details are replaced with [MASKED].
spark.sql("""
    SELECT review_id, ai_mask(review, ARRAY('phone', 'person')) AS safe_review
    FROM reviews
""").display()

# COMMAND ----------

# DBTITLE 1,Example 5: ai_query with your own prompt
# ai_query is the general tool: pick a hosted model endpoint and write any prompt.
# The endpoint name below is a common Databricks-hosted one; change it if your workspace lists a different one
# (Serving page in the sidebar).
spark.sql("""
    SELECT review_id,
           ai_query(
             'databricks-meta-llama-3-3-70b-instruct',
             CONCAT('Reply to this customer review politely in one sentence: ', review)
           ) AS suggested_reply
    FROM reviews
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to read the results
# MAGIC - Each AI function adds a column, so you can mix them with normal SQL: `WHERE`, `GROUP BY`, joins.
# MAGIC - Example: `SELECT ai_analyze_sentiment(review) s, COUNT(*) FROM reviews GROUP BY s` gives a sentiment count.
# MAGIC - Each call to a model costs quota/time, so test on a few rows (`LIMIT 10`) before running on a large table.
# MAGIC - Model output can be wrong, so check it before using it for real decisions.
