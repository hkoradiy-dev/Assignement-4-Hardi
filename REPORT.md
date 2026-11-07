# Assignment #4: Report

**Name: Hardi Koradiya**

**Student ID: 801484363**

**Email:hkoradiy@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801484363

The commands you ran, in order:

```bash
python3 datagen.py 801484363

docker compose up -d

docker compose ps

docker cp datagen.py spark-master:/opt/spark/work-dir/shared/
docker exec spark-master python3 /opt/spark/work-dir/shared/datagen.py 801484363 \
  /opt/spark/work-dir/shared

  docker cp main.py spark-master:/opt/spark/work-dir/

rm -rf shared-folder/checkpoint/part1a
rm -rf shared-folder/output/part1a

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py part1a /opt/spark/work-dir/shared 801484363

rm -rf shared-folder/checkpoint/part1b
rm -rf shared-folder/output/part1b

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  part1b \
  /opt/spark/work-dir/shared \
  801484363

ls shared-folder/output/part1b

docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  part2 \
  /opt/spark/work-dir/shared \
  801484363

docker cp main.py spark-master:/opt/spark/work-dir/
rm -rf shared-folder/checkpoint/part3
rm -rf shared-folder/output/part3


docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  part3 \
  /opt/spark/work-dir/shared \
  801484363

ls shared-folder/output/part3

# to generate the comparison for anamolies
mkdir -p /tmp/a4-truth
python3 datagen.py 801484363 /tmp/a4-truth /tmp/truth.csv

python3 - <<'PY'
import csv
import glob

truth = {
    row["ride_id"]
    for row in csv.DictReader(open("/tmp/truth.csv"))
}

flagged = set()

for file in glob.glob("shared-folder/output/part3/*.csv"):
    with open(file) as f:
        for row in csv.DictReader(f):
            if row.get("ride_id"):
                flagged.add(row["ride_id"])

true_positives = flagged & truth
false_positives = flagged - truth
false_negatives = truth - flagged

precision = len(true_positives) / len(flagged) if flagged else 0
recall = len(true_positives) / len(truth) if truth else 0

print("Total planted anomalies:", len(truth))
print("Total flagged rides:", len(flagged))
print("True positives:", len(true_positives))
print("False positives:", len(false_positives))
print("False negatives:", len(false_negatives))
print("Precision:", round(precision, 4))
print("Recall:", round(recall, 4))
print("False positive ride IDs:", sorted(false_positives))
PY


docker exec -it spark-master /opt/spark/bin/pyspark \
  --master spark://spark-master:7077


  from pyspark.sql.functions import col, window, count, sum as ssum, round as sround

schema = """
ride_id STRING, driver_id STRING, pickup_zone INT,
distance_km DOUBLE, duration_min DOUBLE,
payment_type STRING, fare DOUBLE, event_time TIMESTAMP
"""

stream = spark.read.parquet(
    "/opt/spark/work-dir/shared/output/part1a"
)

batch = (
    spark.read.schema(schema).json(
        "/opt/spark/work-dir/shared/stream"
    )
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("pickup_zone")
    )
    .agg(count("*").alias("trips"))
    .select(col("window.start").alias("window_start"))
    .distinct()
)

print("STREAM WINDOWS:")
stream.select("window_start").distinct().orderBy("window_start").show(100, False)

print("BATCH WINDOWS MISSING FROM STREAM:")
batch.join(
    stream.select("window_start").distinct(),
    "window_start",
    "left_anti"
).orderBy("window_start").show(100, False)
```

---

## Part 1a: trips and revenue per zone per window

The micro-batch summary (batches, watermark, state rows) only some rows are pasted here:

```
The micro-batch summary (batches, watermark, state rows):=== part1a: micro-batch summary ===
batch input rows            watermark state rows
    0         43                    -         23
    1         35  2026-10-27 07:57:57         27
    2         41  2026-10-27 07:58:54         30
    3         42  2026-10-27 07:59:43         31
    4         36  2026-10-27 08:00:52         25
    5         40  2026-10-27 08:01:52         42
    6         35  2026-10-27 08:02:53         48
    7         39  2026-10-27 08:03:49         50
    8         38  2026-10-27 08:04:51         50
    9         37  2026-10-27 08:05:53         25
   10         40  2026-10-27 08:06:55         42
   11         38  2026-10-27 08:07:54         45
   12         42  2026-10-27 08:08:55         47
   13         42  2026-10-27 08:09:58         49


```

Which 5-minute windows did the stream write, and which are missing compared with the batch
table printed after the query? For one window that was written, compare its trips with the
batch value: where did the difference come from (look at how `datagen.py` makes the late
events)? Why does `state rows` drop every five batches?

The stream wrote these six windows:

2026-10-27 07:55:00
2026-10-27 08:00:00
2026-10-27 08:05:00
2026-10-27 08:10:00
2026-10-27 08:15:00
2026-10-27 08:20:00

The following window was present in the batch result but missing from the streaming output:

2026-10-27 08:25:00
For the 08:00 window and pickup zone 5, the streaming result contained 21 trips and $635.31 revenue, while the batch result contained 22 trips and $653.88 revenue. The difference was one ride with a fare of $18.57.

The generator creates very late events that arrive 8–10 minutes late. Because the streaming query uses a three-minute watermark, events arriving behind the watermark can be dropped from the streaming aggregation, while the batch aggregation includes them.

The state rows decrease approximately every five batches because the watermark advances and Spark removes aggregation state for windows that are sufficiently old. This prevents the query from retaining completed window state indefinitely.

## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

```
Total rides processed: 1,185
Total alerts: 21

High-fare alerts: 0
Zero-distance alerts: 21

First five output lines:
=== part1b: micro-batch summary ===
batch input rows            watermark state rows
    0          1                    -          -
    1          0                    -          -
    2          1                    -          -
    3          2                    -          -
    4          0                    -          -
    5          1                    -          -
    6          0                    -          -
    7          0                    -          -
    8          1                    -          -
    9          0                    -          -
   10          2                    -          -
   11          0                    -          -
   12          0                    -          -
   13          1                    -          -
   14          1                    -          -
   15          0                    -          -
   16          0                    -          -
   17          1                    -          -

   === part1b: 21 alerts ===
+-------------+-----+
|       reason|count|
+-------------+-----+
|zero_distance|   21|
+-------------+-----+


```

This query has no watermark and no state. Why is that correct here, and what would change if
you also wanted "more than three alerts for the same driver in ten minutes"?

- This query has no watermark and no state because each ride is evaluated independently. It checks whether the fare is greater than $150 or the distance is zero. No event-time aggregation or historical information is needed.

- If we also wanted more than three alerts for the same driver in ten minutes, the query would need stateful event-time window aggregation. We would group alerts by driver ID and a ten-minute window, count the alerts, and filter groups with counts greater than three. A watermark would help manage late events and remove old aggregation state.

---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the
"tuned" line with the best parameters, and the "best model" line:

```
baseline: RMSE = 3.68, R2 = 0.863

full: RMSE = 1.71, R2 = 0.970

tuned: RMSE = 1.97, R2 = 0.961

Tuned model: RandomForestRegressor
Best parameters:
numTrees = 40
maxDepth = 8

Number of parameter combinations: 4
Number of cross-validation fits: 12

Best model: full
Best model saved successfully at:
shared-folder/models/fare
```

Your tuned model: which estimator, which parameters in the grid and why those, how many fits
the CrossValidator ran, and how long part 2 took. Did the tuning beat the full linear
pipeline? If not, what does that tell you about the data?

The parameter grid tested:
   - numTrees: 20 and 40.
   - maxDepth: 5 and 8.

These values provide a small but useful search across the number of trees and model complexity. The grid produced four parameter combinations. With three-fold cross-validation, the CrossValidator ran 12 fits using parallelism of 2.

Part 2 execution time: Not recorded in the provided output.
- The tuning did not beat the full Linear Regression pipeline. The full pipeline achieved an RMSE of 1.71 and an R² of 0.970, compared with an RMSE of 1.97 and an R² of 0.961 for the tuned Random Forest. This suggests that the relationship between the selected features and fare in this generated dataset is captured effectively by the linear model, and the more complex model did not improve test performance.



Why is `hour` turned into a string before `StringIndexer` (see `features_of`) instead of
being used as a number?
- The hour feature is converted to a string before StringIndexer because the hour represents a category of the day rather than a continuous numerical measurement. Treating it as a category allows the model to learn different effects for different hours instead of assuming a linear relationship between hour number and fare.

---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation
and the per-window counts:

```
Threshold: 6.84
Total rides flagged: 53

Top ten rides by deviation:

R00291 - 89.52
R00193 - 77.20
R00593 - 41.84
R01087 - 34.70
R00466 - 33.06
R00766 - 32.18
R00973 - 30.52
R00748 - 29.11
R01057 - 28.94
R01109 - 28.19

Flagged rides per window:

08:00:00 - 9
08:05:00 - 10
08:10:00 - 7
08:15:00 - 9
08:20:00 - 7
08:25:00 - 11

Total - 53

The planted anomaly comparison produced:

Total planted anomalies: 35
Total flagged rides: 53
True positives: 35
False positives: 18
False negatives: 0

Precision: 0.6604
Recall: 1.0000

All 35 planted anomalies were detected, and 18 additional rides were flagged even though they were not planted anomalies.

The false-positive ride IDs were:
R00027, R00194, R00202, R00311, R00352, R00417,
R00492, R00535, R00732, R00745, R00761, R00789,
R00835, R00845, R01048, R01111, R01131, R01176
```

Run `python3 datagen.py <seed> shared-folder /tmp/truth.csv` (the third argument writes the
list of planted anomalies, which the generator does not reveal otherwise) and compare it with
your flagged rides: how many of the planted anomalies did you catch, and how many flagged
rides were not planted? Look at the false positives: what do they have in common, and what
would you change in part 2 to reduce them?

- These false positives are rides whose prediction deviations exceeded the threshold even though they were not planted anomalies. This indicates that the model does not fully capture every legitimate fare variation.

To reduce false positives, I would consider adding more useful predictive features, evaluating alternative models, and tuning the anomaly threshold using validation data while trying to preserve the current recall


Why is there no `fit()` in part 3, and what would happen if you tried one on the stream?
Which stages of the loaded model are fitted models, and what did each one learn from the
training data?

There is no fit() in Part 3 because the model was already trained in Part 2 and saved as a PipelineModel. Part 3 loads this fitted model and uses it to make predictions on streaming rides. Training is a batch operation, whereas Part 3 performs streaming inference. Attempting to fit a model directly on an unbounded stream is not supported by the normal Spark ML training workflow.

The loaded model contains these fitted stages:

StringIndexerModel: learned the mappings from categorical values to numerical indices.

OneHotEncoderModel: learned the categorical encoding layout.

VectorAssembler: combines the encoded categorical features and numerical features into a feature vector.

LinearRegressionModel: learned the regression coefficients and intercept used to predict fares.


---

## Design: this pipeline in production

One paragraph. The platform runs this for real: thousands of rides a minute from Kafka, a
model retrained every night on the trips of the last month, anomalies sent to a fraud team.
Where do the training job, the scoring job, the checkpoint and the model live, how does a
new model reach the running scoring job without losing events, and what would you monitor
to know that the model has gone stale?


In production, ride events would be ingested through Apache Kafka and processed using Spark Structured Streaming. The scoring job would run continuously and perform fare prediction and anomaly detection. 

A separate scheduled training job would run nightly using the previous month's ride data, evaluate the new model, and save the validated model as a versioned artifact in durable storage or a model registry. 

The streaming scoring job would read the deployed model, while its checkpoint would be stored in reliable distributed storage to preserve streaming progress and state across failures. To deploy a new model without losing events, the new version would be validated first and then introduced through a controlled model-version update or scoring-job restart while preserving the checkpoint and Kafka offsets. 

Detected anomalies would be sent to a fraud team through an alert queue or dashboard. Monitoring would include prediction accuracy, precision, recall, false-positive rates, data drift, processing latency, Kafka lag, watermark progress, and streaming failures to identify when the model becomes stale.


---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.


The initial Spark execution failed with:

ModuleNotFoundError: No module named 'numpy'

Installing NumPy initially also produced a permission error involving:

Permission denied: '/nonexistent'

The issue was resolved by installing NumPy with root permissions inside the Spark Docker containers so the driver and worker environments could access the dependency.

After resolving the dependency issue, all four assignment parts executed successfully and generated their expected outputs.