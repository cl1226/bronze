# Bronze
Bronze架构于Apache SparkMllib之上的机器学习平台，提供数据接入，转换，训练，测试，输出。

-----
### Bronze结构

| 数据采集 |
| ----- |

&darr;

| 数据预处理 |
| ----- |

&darr;

| 特征工程 |
| ----- |

&darr;

| 模型训练 |
| ----- |

&darr;

| 模型验证 |
| ----- |

&darr;

| 模型持久化 |
| ----- |

&darr;

| 模型结果输出 |
| ----- |

### Bronze支持的算法模型
##### 分类模型

| 模型       | 特征数量     | 训练样例数 | 输出类别               |
| ---------- | ------------ | ---------- | ---------------------- |
| 逻辑回归   | 100万~1000万 | 无限       | 特征 × 类别数 < 1000万 |
| 决策树     | 1000         | 无限       | 特征 × 类别数 < 10000  |
| 随机森林   | 10000        | 无限       | 特征 × 类别数 < 100000 |
| 梯度提升树 | 1000         | 无限       | 特征 × 类别数 < 10000  |

##### 回归模型

| 模型         | 特征数量      | 训练样例数 |
| ------------ | ------------- | ---------- |
| 线性回归     | 100万~10000万 | 无限       |
| 广义线性回归 | 4096          | 无限       |
| 保序回归     | N/A           | 百万级别   |
| 决策树       | 1000          | 无限       |
| 随机森林     | 10000         | 无限       |
| 梯度提升树   | 1000          | 无限       |
| 存活分析     | 100万~1000万  | 无限       |

##### 聚类

| 模型        | 建议           | 计算限制              | 训练例子 |
| ----------- | -------------- | --------------------- | -------- |
| k-means     | 最大值为50~100 | 特征乘以聚类 < 1000万 | 不限     |
| 二分k-means | 最大值为50~100 | 特征乘以聚类 < 1000万 | 不限     |
| GMM         | 最大值为50~100 | 特征乘以聚类 < 1000万 | 不限     |
| LDA         | 可解释的数字   | 1000个主题            | 不限     |



------

### Bronze 支持的插件

#### Input Plugin

   - Fake 
   - File 
   - HDFS

#### Transform Plugin
   ##### 通用转换器
   - Filter *过滤*
   - Schema *将输入数据集结构化*
   - Sql *通过sql方式操作数据集*
   - LabeledPoint *生成标注点*
   - Split *数据集切分为训练集、测试集*
   - TypeConvert *类型转换*
   - Rename *列值重命名*
   - Sample *抽样*
   ##### 特征选择(Feature Selections)
>在特征向量中选择出那些”优秀“的特征，组成新的、更”精简“的特征向量的过程。

- VectorSlicer *输入特征向量，输出原始特征向量子集*

- RFormula *允许在声明式语言指定转换*

- ChiSqSelector *精简特征向量*

   ##### 特征转换(Feature Transforms)
   ###### 特征索引转换
   - StringIndexer *将字符串映射到不同的数字id*
   - IndexToString *将索引的数值类型映射回原始的类别之*
   - VectorIndexer *向量索引化*
   ###### 文档转换
   - Tokenizer *文本分词*
   - StopWordRemover *删除常用词*
   - NGram *单词组合，即长度为n的单词序列*
   ###### 正则化
   - Normalizer *使用某个幂范数来缩放多维向量*
   ###### 标准化
   - StandardScaler *将一组特征值归一化为平均值为0而标准偏差为1的一组新值*
   ###### 归一化
   - MinMaxScaler *基于给定的最小值到最大值按比例缩放*
   - MaxAbsScaler *将每个值除以该特征的最大绝对值来缩放数据*
   ###### 二值化
   - Binarizer *特征值大于该阈值时为1, 小于等于则为0*
   ###### 离散化
   - Bucketizer *将连续的数据放入已经设置好的n个桶中*
   - QuantileDiscretizer *根据设置的桶的数量对连续变量进行拆分，且它可以处理NaN值*
   ###### 独热编码
   - OneHotEncoder *将分类转换为二进制向量, 该向量中只有一位为1，其他为0*
   ###### 缺失值补全
   - Imputer *对列中的NaN使用中位数或者平均数进行补全*
   ###### 特征构造
   - PolynomialExpansion *将特征进行多项式展开形成新的特征*
   - Interaction *将多列向量构造成一个向量，新向量的内容是每个输入列的每个值与其他列组合的乘积*
   - ElementwiseProduct *通过将向量列与一个标量列进行乘积来对向量进行缩放*
   - VectorAssembler *对输入列进行拼接生成一个新的向量*
   ###### 降维
   - PCA *主成分分析*
   ###### 其他
   - SQLTransformer *使用sql的方式对数据做转换，不需要是要表名，而是使用关键字__THIS__*

   ##### 特征提取(Feature Extractors)
   - TF-IDF *词频-逆向文件频率，提取文档关键词*
   - Word2Vec *将词语或者文章转换成词向量*
   - CountVectorizer *类似TF-IDF*
   - FeatureHasher *特征哈希，将原来的N维特征转换为一个M维的特征向量，一般M<N*

#### Train Plugin
  ##### 分类模型
   - LogisticRegression *逻辑回归*
   - DecisionTree *决策树*
   - RandomForest *随机森林*
   - GBT *梯度提升树*
   - LinearSVC *线性支持向量机*
   - NaiveBayes *朴素贝叶斯*
  ##### 回归模型
   - LinearRegression *线性回归*
   - GBTRegressor *梯度提升树回归*
   - DecisionTree *决策树*
   - GeneralizedLinearRegression *广义线性回归*
   - RandomForest *随机森林*

##### 聚类模型

 - Kmeans
 - BisectingKMeans *二分k-means*
 - GaussianMixture *GMM*

##### 推荐系统

 - ALS *交替最小二乘法*

#### Test Plugin
   - BinaryClassificationValidate *二分类模型验证*
   - MultiClassificationValidate *多分类模型验证*
   - LinearRegressionValidate *线性回归模型验证*
   - GeneralizedLinearRegressionValidate *广义线性回归模型验证*
   - RegressionValidate *通用回归模型验证*
   - ClusteringValidate *聚类模型验证*

#### Output Plugin

   - Stdout

### 环境依赖
1. Java运行环境，JDK Version >= 8
2. 如果要在集群环境中运行
    - Spark on Yarn