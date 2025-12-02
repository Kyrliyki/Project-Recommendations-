import time
import pandas as pd

from src.scripts_utils.get_data import get_data
from src.ml_models.matrix_factorization_based_cf.model import MLMatrixFactorizationSVD
from src.utils.config import settings


def main():
    start_time = time.time()

    train, validation, test = get_data()

    print("Поиск лучших параметров для SVD...")
    best_params = MLMatrixFactorizationSVD.search_best_params(train)
    print("Параметры найдены.")

    print("Загрузка лучших параметров в csv...")
    pd.DataFrame(best_params["rmse"]).to_csv(settings.ml.svd_best_params_for_rmse, index=False)
    pd.DataFrame(best_params["mae"]).to_csv(settings.ml.svd_best_params_for_mae, index=False)
    print("Параметры загружены.")

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nВремя выполнения: {duration:.6f} секунд")


if __name__=="__main__":
    main()