# pug2pug

Cloud Dataflowを使って、Cloud DatastoreのMigrationを行う。
単純にDatastoreのデータを引っ越すだけなら、 [Cloud Datastore Import/Export](https://cloud.google.com/datastore/docs/export-import-entities) を使ったほうがよい。

## 存在する機能

* Datastoreを別プロジェクトにお引っ越し
* EntityのProperty名の変更

## 未対応

* Datastore Namespace
* Entity Filter

## Run

```
mvn compile exec:java -Dexec.mainClass=org.sinmetal.beam.ds2ds.DatastoreToDatastore -Dexec.args="--runner=DataflowRunner --project={source project id} \
     --inputKinds={カンマ区切りのKind一覧 example : hoge,fuga,moge} \
     --inputProjectId={source project id} --outputProjectId={target project id} --tempLocation={tmp cloud storage bucket}" -Pdataflow-runner
```