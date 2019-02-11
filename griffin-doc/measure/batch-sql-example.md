input table <events>:

```
id                  	int
unix_time           	int
category_id         	int
ip                  	string
type                	string
dt                  	string
```


Example of category_id range checking using 3 sigma. We expect that less 5% values exceed 3 sigma.


```
{
  "name": "batch_sigma",

  "process.type": "batch",

  "data.sources": [
    {
      "name": "src",
      "baseline": true,
      "connectors": [
        {
          "type": "hive",
          "version": "1.2",
          "config": {
            "database": "default",
            "table.name": "events"
          }
        }
      ]
    }
  ],
  "evaluate.rule": {
    "rules": [
      {
        "dsl.type": "spark-sql",
        "out.dataframe.name": "out1",
        "rule": "SELECT AVG(category_id) AS avg, MIN(category_id) AS mina, MAX(category_id) AS maxa, count(*) AS cnt FROM src",
        "out": []
      },
      {
        "dsl.type": "spark-sql",
        "out.dataframe.name": "out2",
        "rule": "SELECT b.*, (b.category_id - b.avg) AS category_id_normal FROM (SELECT src.*, out1.* FROM out1 LEFT JOIN src) AS b",
        "out": []
      },
      {
        "dsl.type": "spark-sql",
        "out.dataframe.name": "out3",
        "rule": "SELECT POW(SUM(POW(out2.category_id_normal, 2)) / MAX(out2.cnt), 0.5) AS sigma FROM out2",
        "out": []
      },
      {
        "dsl.type": "spark-sql",
        "out.dataframe.name": "outside_3s",
        "rule": "SELECT out2.*, out3.* FROM out2 LEFT JOIN out3 WHERE out2.category_id > out2.avg + 3*out3.sigma OR out2.category_id < out2.avg - 3*out3.sigma",
        "out": [
          {
            "type": "record",
            "name": "metric_out_3s"
          }
        ]
      },
      {
        "dsl.type": "spark-sql",
        "out.dataframe.name": "res",
        "rule": "SELECT b.percent_outside, b.cnt_outside, b.percent_outside < 5 AS success FROM (SELECT count(*) AS cnt_outside, count(*)/MAX(cnt)*100 AS percent_outside FROM outside_3s) AS b",
        "out": [
          {
            "type": "metric",
            "name": "res_metric"
          },
          {
            "type": "record",
            "name": "res_records"
          }
          
        ]
      }

    ]
  },

  "sinks": ["CONSOLE", "HDFS"]
}
```
