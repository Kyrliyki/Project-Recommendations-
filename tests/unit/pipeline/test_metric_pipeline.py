import pandas as pd
import pytest

from src.pipelines.metric_pipeline import MetricPipeline






def test_metric_pipeline_initialization():
    """Тест корректной инициализации пайплайна"""
    pipeline = MetricPipeline(k_list=[5, 10, 3], metrics=['Precision', 'Recall'])
    assert pipeline.k_list == [3, 5, 10]
    assert 'Precision' in pipeline.metrics

def test_metric_pipeline_initialization_with_default_metrics():
    """Тест инициализации без указания метрик (все доступные)"""
    pipeline = MetricPipeline(k_list=[5, 10])
    assert pipeline.metrics == ['Precision', 'Recall', 'MAP', 'NDCG']
    assert pipeline.available_metrics is not None

def test_metric_pipeline_invalid_metric():
    """Тест на ошибку при указании несуществующей метрики"""
    with pytest.raises(ValueError, match="Метрика 'InvalidMetric' не реализована"):
        MetricPipeline(k_list=[5], metrics=['Precision', 'InvalidMetric'])

def test_metric_pipeline_empty_k_list():
    """Тест с пустым списком k"""
    pipeline = MetricPipeline(k_list=[], metrics=['Precision'])
    assert pipeline.k_list == []



def test_run_multiple_models(basic_recommendation_scenario_for_metric_pipeline):
    """Тест запуска пайплайна для нескольких моделей"""
    pipeline = MetricPipeline(k_list=[3], metrics=['Precision'])
    scenario = basic_recommendation_scenario_for_metric_pipeline

    result_df = pipeline.run(
        scenario['recommendations'],
        scenario['relevant']
    )

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.index.name == 'Model'
    assert 'Model1' in result_df.index
    assert 'Model2' in result_df.index
    assert 'Precision@3' in result_df.columns

def test_run_single_model(perfect_recommendation_scenario_for_metric_pipeline):
    """Тест запуска пайплайна для одной модели"""
    pipeline = MetricPipeline(k_list=[3], metrics=['Precision', 'Recall'])
    scenario = perfect_recommendation_scenario_for_metric_pipeline

    result_df = pipeline.run(
        scenario['recommendations'],
        scenario['relevant']
    )

    assert len(result_df) == 1
    assert 'PerfectModel' in result_df.index

def test_run_empty_recommendations(empty_recommendation_scenario_for_metric_pipeline):
    """Тест с пустыми рекомендациями"""
    pipeline = MetricPipeline(k_list=[3], metrics=['Precision'])
    scenario = empty_recommendation_scenario_for_metric_pipeline

    result_df = pipeline.run(
        scenario['recommendations'],
        scenario['relevant']
    )

    assert not result_df.empty


def test_calculate_metrics_for_model_basic():
    """Тест расчета всех метрик для модели"""
    pipeline = MetricPipeline(k_list=[2, 3], metrics=['Precision', 'Recall'])

    # Mock метрик
    def mock_precision(recommended, relevant, k):
        return 0.5

    def mock_recall(recommended, relevant, k):
        return 0.3

    pipeline.available_metrics = {
        'Precision': mock_precision,
        'Recall': mock_recall
    }

    recommendations = [[1, 2, 3], [4, 5, 6]]
    relevant = [[1, 2], [4, 5]]

    result = pipeline.calculate_metrics_for_model(
        'TestModel', recommendations, relevant
    )

    assert 'TestModel' in result
    assert 'Precision@2' in result['TestModel']
    assert 'Recall@3' in result['TestModel']


def test_calculate_metrics_for_model_single_k():
    """Тест расчета метрик для одного значения k"""
    pipeline = MetricPipeline(k_list=[5], metrics=['Precision'])

    result = pipeline.calculate_metrics_for_model(
        'Model', [[1, 2, 3]], [[1, 2]]
    )

    assert len(result['Model']) == 1