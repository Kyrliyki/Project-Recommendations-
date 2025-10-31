import dask.dataframe as dd


class Metrics:
    @staticmethod
    def rmse_k(
            test_data: dd.DataFrame,
            k: int,
    ) -> float:
        """
        подсчет RMSE@K
            test_data: dd.DataFrame - данные для тестирования
            k: int - количество первых результатов для тестирования
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def precision_k(
            test_data: dd.DataFrame,
            k: int,
    ) -> float:
        """
        подсчет Precision@K
            test_data: dd.DataFrame - данные для тестирования
            k: int - количество первых результатов для тестирования
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def recall_k(
            test_data: dd.DataFrame,
            k: int,
    ) -> float:
        """
        подсчет Recall@K
            test_data: dd.DataFrame - данные для тестирования
            k: int - количество первых результатов для тестирования
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def map_k(
            test_data: dd.DataFrame,
            k: int,
    ) -> float:
        """
        подсчет MAP@K
            test_data: dd.DataFrame - данные для тестирования
            k: int - количество первых результатов для тестирования
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def ndcg_k(
            test_data: dd.DataFrame,
            k: int,
    ) -> float:
        """
        подсчет NDCG@K
            test_data: dd.DataFrame - данные для тестирования
            k: int - количество первых результатов для тестирования
        returning
            значение подсчитанной метрики: float
        """
        pass