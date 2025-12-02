from pathlib import Path
from typing import Type
import pandas as pd

import dask.dataframe as dd
import pickle


def get_model(
        model_cls: Type,
        model_pkl: Path,
        train: dd.DataFrame,
        model_best_params: Path| None = None,
):
    print("\nПроверка наличия сохранённой модели...")
    if model_pkl.exists():
        print("Сохранённая модель найдена. Загружаем...")
        try:
            with model_pkl.open("rb") as f:
                model = pickle.load(f)
            print("Модель загружена из", model_pkl)
        except Exception as e:
            print(f"Ошибка при загрузке модели: {e}. Обучаем заново...")
            model = model_cls()
            model.fit(train)
            print("Модель обучена!")

            try:
                with model_pkl.open("wb") as f:
                    pickle.dump(model, f)
                print("Модель сохранена в", model_pkl)
            except Exception as e:
                print(f"Не удалось сохранить модель: {e}")
    else:
        print("Сохранённой модели не найдено. Обучаем...")
        if model_best_params is None:
            model = model_cls()
        else:
            params = pd.read_csv(model_best_params).to_dict("records")[0]
            model = model_cls(
                **params
            )
        model.fit(train)
        print("Модель обучена!")

        try:
            with model_pkl.open("wb") as f:
                pickle.dump(model, f)
            print("Модель сохранена в", model_pkl)
        except Exception as e:
            print(f"Не удалось сохранить модель: {e}")

    return model