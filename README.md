## Run map demo: 
```
mvn clean package
spark-submit --class map.Main target/.../..jar
```

## Run filter demo: 
```
mvn clean package
spark-submit --class filter.Main target/.../..jar
```

## Run groupByKey demo: 
```
mvn clean package
spark-submit --class groupbykey.Main target/.../..jar
```

## Run reduceByKey demo: 
```
mvn clean package
spark-submit --class reducebykey.Main target/.../..jar
```
