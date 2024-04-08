import argparse
import os

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassifier
import mlflow

LABEL_COL = 'has_car_accident'


def build_pipeline(train_alg):
    """
    Создание пайплаина над выбранной моделью.

    :return: Pipeline
    """
    sex_indexer = StringIndexer(inputCol='sex',
                                outputCol="sex_index")
    car_class_indexer = StringIndexer(inputCol='car_class',
                                      outputCol="car_class_index")
    features = ["age", "sex_index", "car_class_index", "driving_experience",
                "speeding_penalties", "parking_penalties", "total_car_accident"]
    assembler = VectorAssembler(inputCols=features, outputCol='features')
    return Pipeline(stages=[sex_indexer, car_class_indexer, assembler, train_alg])


def evaluate_model(evaluator, predict, metric_list):
    metrics_list = []
    for metric in metric_list:
        evaluator.setMetricName(metric)
        score = evaluator.evaluate(predict)
        metrics_list.append(score)
        print(f"{metric} score = {score}")
    return metrics_list


def optimization(pipeline, gbt, train_df, evaluator):
    grid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [3, 5]) \
        .addGrid(gbt.maxIter, [20, 30]) \
        .addGrid(gbt.maxBins, [16, 32]) \
        .build()
    tvs = TrainValidationSplit(estimator=pipeline,
                               estimatorParamMaps=grid,
                               evaluator=evaluator,
                               trainRatio=0.8)
    models = tvs.fit(train_df)
    return models.bestModel


def process(spark, train_path, test_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param train_path: путь до тренировочного датасета
    :param test_path: путь до тренировочного датасета
    """
    # my code start 
    mlflow.start_run()
    
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = '...'
    os.environ['AWS_ACCESS_KEY_ID'] = '...'
    os.environ['AWS_SECRET_ACCESS_KEY'] = '...'
    
    mlflow.set_tracking_uri("https://mlflow.lab.karpov.courses")
    mlflow.set_experiment(experiment_name = "g-maslov")    
    # my code end 
    
    evaluator = MulticlassClassificationEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName='f1')
    train_df = spark.read.parquet(train_path)
    test_df = spark.read.parquet(test_path)

    gbt = GBTClassifier(labelCol=LABEL_COL)
    pipeline = build_pipeline(gbt)

    model = optimization(pipeline, gbt, train_df, evaluator)
    predict = model.transform(test_df)

    metrics_list = evaluate_model(evaluator, predict, ['f1', 'weightedPrecision', 'weightedRecall', 'accuracy'])
    # my code start 
    f1 = metrics_list[0]
    weightedPrecision = metrics_list[1]
    weightedRecall = metrics_list[2]
    accuracy = metrics_list[3]
    # my code end
    print('Best model saved')
    
    # my code start 
    stage = p_model.stages[0]
    mlflow.log_param(f'input_columns', stage.getInputCols())
    mlflow.log_param(f'target', 'has_car_accident')
    
    for i in range(0, len(p_model.stages)):
        stage = p_model.stages[i]
        mlflow.log_param(f'stage_{i}', stage)
        if type(stage) is VectorAssembler:
            mlflow.log_param(f'features', stage.getInputCols())
    
    mlflow.log_param('MaxDepth', p_model.stages[-1].getMaxDepth())
    mlflow.log_param('maxIter', p_model.stages[-1].getmaxIter())
    mlflow.log_param('MaxBins', p_model.stages[-1].getMaxBins())
    
    mlflow.log_metric('f1', f1)
    mlflow.log_metric('weightedPrecision', weightedPrecision)
    mlflow.log_metric('weightedRecall', weightedRecall)
    mlflow.log_metric('accuracy', accuracy)
    
    mv = mlflow.spark.log_model(p_model,
                            artifact_path = "g-maslov",
                            registered_model_name="g-maslov")
    mlflow.end_run()
    # my code end 

def main(train_path, test_path):
    spark = _spark_session()
    process(spark, train_path, test_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkMLJob').getOrCreate()


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=str, default='train.parquet', help='Please set train datasets path.')
    parser.add_argument('--test', type=str, default='test.parquet', help='Please set test datasets path.')
    args = parser.parse_args()
    train = args.train
    test = args.test
    main(train, test)