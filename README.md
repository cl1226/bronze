# Bronze
Bronze架构于Apache SparkMllib之上的机器学习平台，提供数据接入，转换，训练，测试

-----

### Bronze 支持的插件

- Input Plugin

Fake, File, HDFS

- Transform Plugin

   - Filter *过滤*
   - Schema *给输入数据集增加schema*
   - Sql *通过sql方式操作员数据集*
   - StringIndexer
   - LabeledPoint
   - Tokenizer 
   - StandardScaler 
   - RFormula
   - VectorAssembler
   - StopWordRemover

- Machine learning Plugin

LinearRegression, LogisticRegression

- Output Plugin

Stdout

### 环境依赖
1. Java运行环境，JDK Version >= 8
2. 如果要在集群环境中运行
    - Spark on Yarn