import numpy as np
import dask.dataframe as dd
import time

from ..config import settings

from ..ml_models.item_based_cf.model import MLItemBasedCFSimple
from ..preparing_data import (
    download_csv,
    train_validation_test_split_ddf,
)

start_time = time.time()

print("\nЗагрузка датасета...")
download_csv(
    input_folder_path=settings.data.input_folder_path,
    url=settings.data.dataset_url
)
print("Датасет загружен!")


print("\n   Деление на train, validation, test...")
df = dd.read_csv(
    settings.data.path_to_rating_csv,
    parse_dates=[settings.data.column_names.timestamp],
)
train, validation, test = train_validation_test_split_ddf(df)
print("\n   Датасет поделен на train, validation, test выборки!")


print("\n   Обучение модели...")
model = MLItemBasedCFSimple()
model.fit(train)
print("\n   Модель обучена!")


print("\n   Тестирование модели...")

users = np.asarray(
    validation[
        settings.data.column_names.userId
    ].unique().head(
        settings.metrics.n_users
    )
)

for user in users:
    relevant_for_current_user = np.asarray(
        validation[
            (validation[settings.data.column_names.userId] == user) &
            (validation[settings.data.column_names.rating] > settings.metrics.threshold_for_binarize)
        ].sort_values(
            by=settings.data.column_names.rating,
            ascending=False
        )[settings.data.column_names.movieId]
    )
    recommend_for_current_user = model.getting_recommended_movies(
        user_id=user,
        top_k=50,
    )
    print(f"\nПользователь: {user}")
    print(f"Relevant: {relevant_for_current_user}")
    print(f"Recommend (первые 10): {recommend_for_current_user[:10]}")