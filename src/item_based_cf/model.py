import pandas as pd
import numpy as np
import scipy.sparse as sp
import dask.dataframe as dd
from typing import List, Dict, Optional
from tqdm import tqdm

from implicit.nearest_neighbours import ItemItemRecommender

from src.schemes.model_base import MLModelBase, MetricsScheme
from src.config import settings


class MLItemBasedCFImplicit(MLModelBase):
    def __init__(self, top_k: int = 50):
        self.model = ItemItemRecommender(K=top_k)
        self.user_map: Dict[int, object] = {}  # idx -> original user_id
        self.item_map: Dict[int, object] = {}  # idx -> original item_id
        self.user_index: Dict[object, int] = {}  # original user_id -> idx
        self.item_index: Dict[object, int] = {}  # original item_id -> idx
        self.user_item_matrix: Optional[sp.csr_matrix] = None
        self.top_k = top_k

    def _to_pandas(self, data):
        if isinstance(data, dd.DataFrame):
            return data.compute()
        return data

    def _prepare_matrix(self, data: pd.DataFrame, use_existing_maps: bool = False) -> sp.csr_matrix:
        """
        Build CSR matrix with fixed shape = (n_users_train, n_items_train) when use_existing_maps=True.
        If use_existing_maps=False -> create new maps from data.
        """
        u_col = settings.data.column_names.userId
        i_col = settings.data.column_names.movieId
        r_col = settings.data.column_names.rating

        # ensure dataframe has expected columns
        data = data[[u_col, i_col, r_col]].copy()

        if not use_existing_maps:
            # create category codes and maps
            users_cat = data[u_col].astype("category")
            items_cat = data[i_col].astype("category")

            user_codes = users_cat.cat.codes.values
            item_codes = items_cat.cat.codes.values

            # maps: idx -> id
            self.user_map = dict(enumerate(users_cat.cat.categories))
            self.item_map = dict(enumerate(items_cat.cat.categories))
            # reverse maps: id -> idx
            self.user_index = {v: k for k, v in self.user_map.items()}
            self.item_index = {v: k for k, v in self.item_map.items()}
        else:
            # use existing maps: keep only rows that are known in train
            user_to_idx = self.user_index
            item_to_idx = self.item_index
            mask = data[u_col].isin(user_to_idx) & data[i_col].isin(item_to_idx)
            if not mask.any():
                # return empty matrix of train shape
                return sp.csr_matrix((len(self.user_map), len(self.item_map)))
            data = data.loc[mask]
            user_codes = data[u_col].map(user_to_idx).values
            item_codes = data[i_col].map(item_to_idx).values

        values = data[r_col].astype(float).values
        matrix = sp.csr_matrix(
            (values, (user_codes, item_codes)),
            shape=(len(self.user_map), len(self.item_map)),
        )
        return matrix

    def fit(self, data) -> None:
        """
        data: dask or pandas dataframe with columns userId, movieId, rating
        """
        df = self._to_pandas(data)
        self.user_item_matrix = self._prepare_matrix(df, use_existing_maps=False)
        # ItemItemRecommender expects item-user matrix
        self.model.fit(self.user_item_matrix.T.tocsr())

    def getting_recommended_movies(self, user_id: int, expected_number_of_recommendations: int) -> List[int]:
        """
        Возвращает список оригинальных item_id.
        Если пользователь неизвестен — возвращает популярные товары.
        """
        if user_id not in self.user_index:
            if self.user_item_matrix is None:
                return []
            pop = np.asarray(self.user_item_matrix.getnnz(axis=0)).ravel()
            top_idx = np.argsort(pop)[::-1][:expected_number_of_recommendations]
            return [self.item_map[int(i)] for i in top_idx]

        u_idx = self.user_index[user_id]
        recs = self.model.recommend(
            u_idx,
            self.user_item_matrix,
            N=expected_number_of_recommendations,
            filter_already_liked_items=True
        )

        # В ItemItemRecommender recs — это просто массив индексов
        if isinstance(recs, np.ndarray) or isinstance(recs, list):
            item_indices = [int(i) for i in recs]
        else:
            item_indices = [int(i) for i, *_ in recs]

        return [self.item_map[int(i)] for i in item_indices]

    def calculating_metrics(self, test, max_eval_users=5000):
        print("Расчет метрик...")

        train_csr = self.user_item_matrix
        test_csr = self._prepare_matrix(test, use_existing_maps=True)
        eval_users = np.arange(min(max_eval_users, train_csr.shape[0]))

        precisions, recalls, maps, ndcgs = [], [], [], []

        for user in tqdm(eval_users, desc="Eval users"):
            try:
                recs = self.model.recommend(
                    user,
                    train_csr,
                    N=self.top_k,
                    filter_already_liked_items=True
                )
            except Exception as e:
                print(f"Ошибка при рекомендации для пользователя {user}: {e}")
                continue

            # recs может быть просто списком индексов
            if isinstance(recs, (list, np.ndarray)):
                rec_item_idxs = [int(i) for i in recs]
            elif isinstance(recs, (tuple, list)) and len(recs) > 0 and isinstance(recs[0], (tuple, list)):
                rec_item_idxs = [int(i) for i, *_ in recs]
            else:
                continue

            test_items = test_csr[user].indices
            if len(test_items) == 0:
                continue

            hit_set = set(rec_item_idxs) & set(test_items)
            precision = len(hit_set) / len(rec_item_idxs)
            recall = len(hit_set) / len(test_items)

            precisions.append(precision)
            recalls.append(recall)

            # MAP@K
            ap = 0.0
            hit_count = 0
            for idx, item in enumerate(rec_item_idxs, 1):
                if item in test_items:
                    hit_count += 1
                    ap += hit_count / idx
            maps.append(ap / min(len(test_items), self.top_k))

            # NDCG@K
            dcg = sum(1 / np.log2(i + 2) for i, item in enumerate(rec_item_idxs) if item in test_items)
            idcg = sum(1 / np.log2(i + 2) for i in range(min(len(test_items), self.top_k)))
            ndcgs.append(dcg / idcg if idcg > 0 else 0)

        metrics = {
            "precision@k": np.mean(precisions) if precisions else 0.0,
            "recall@k": np.mean(recalls) if recalls else 0.0,
            "map@k": np.mean(maps) if maps else 0.0,
            "ndcg@k": np.mean(ndcgs) if ndcgs else 0.0,
        }

        print("Метрики рассчитаны успешно:")
        for k, v in metrics.items():
            print(f"  {k}: {v:.4f}")

        return metrics


if __name__ == "__main__":
    # Загружаем train/test
    train = dd.read_csv(settings.data.csv_save_train_path).compute()
    test = dd.read_csv(settings.data.csv_save_test_path).compute()

    u_col = settings.data.column_names.userId
    i_col = settings.data.column_names.movieId

    # Проверка пересечения
    train_users = set(train[u_col])
    train_items = set(train[i_col])
    test_users = set(test[u_col])
    test_items = set(test[i_col])

    missing_users = test_users - train_users
    missing_items = test_items - train_items

    if missing_users:
        print(f"Внимание! В test есть пользователи, которых нет в train: {len(missing_users)}")
    if missing_items:
        print(f"Внимание! В test есть фильмы, которых нет в train: {len(missing_items)}")

    # Фильтруем test, оставляем только известные пользователи и фильмы
    test_filtered = test[test[u_col].isin(train_users) & test[i_col].isin(train_items)]

    print(f"Test после фильтрации: {len(test_filtered)} строк")

    model = MLItemBasedCFImplicit(top_k=50)
    model.fit(train)

    metrics = model.calculating_metrics(test_filtered, max_eval_users=5000)
    print("Метрики модели:")
    print(metrics)

    sample_user_id = train[u_col].iloc[0]
    recs = model.getting_recommended_movies(sample_user_id, 5)
    print(f"\nРекомендации для пользователя {sample_user_id}: {recs}")
