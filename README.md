# Spanner Skyline [WIP]

This project uploads local files to Google Cloud Spanner

## Examples

#### Exec

```bash
go run main.go -folder=./data -credentials=/home/name/.key/googlekey.json -project=my-google-project -instance=my-spanner-instance -database=my-database -table=my-table
```

#### Data

1. ./data/spanner_data_1.txt
2. ./data/spanner_data_2.txt
3. ./data/spanner_data_3.txt
4. ./data/spanner_data_<...>.txt


```json
{"Id":"546","Flow":"1","Name":"a","Date":"2010-01-01T02:00:02.000Z"}
{"Id":"473","Flow":"2","Name":"b","Date":"2010-01-01T02:00:04.000Z"}
{"Id":"742","Flow":"3","Name":"c","Date":"2010-01-01T02:00:06.000Z"}
{"Id":"238","Flow":"4","Name":"d","Date":"2010-01-01T02:00:08.000Z"}
```

#### Table Schema

```sql
CREATE TABLE MyTable (
    Id INT64 NOT NULL,
    Flow INT64 NOT NULL,
    EventDate TIMESTAMP NOT NULL,
    Name STRING(MAX) NOT NULL,
) PRIMARY KEY (Id, Flow, EventDate)
```