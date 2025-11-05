from typing import List
import dask.dataframe as dd
from surprise import Dataset, Reader, KNNBasic, Prediction
from surprise.model_selection import train_test_split
from tqdm import tqdm
from ..model_base import MLModelBase
from src.utils.config import settings


class MLItemBasedCFSimple(MLModelBase):
    model_name = "IBCF"
    def __init__(self) -> None:
        # sim_options = {"name": "cosine", "user_based": False} => item-based CF
        self.sim_options = {"name": "cosine", "user_based": False}
        self.model = KNNBasic(sim_options=self.sim_options, verbose = True)

    @staticmethod
    def _load_dataset(data: dd.DataFrame):
        reader = Reader(rating_scale=(1, 5))
        df = data.compute() if isinstance(data, dd.DataFrame) else data
        dataset = Dataset.load_from_df(df[[
            settings.data.column_names.userId,
            settings.data.column_names.movieId,
            settings.data.column_names.rating,
        ]], reader)
        return dataset

    def fit(self, data: dd.DataFrame) -> None:
        dataset = self._load_dataset(data).build_full_trainset()
        self.model.fit(dataset)

    def predict(
            self,
            user_id: int,
            movie_id: int,
    ) -> Prediction:
        predict = self.model.predict(
            uid=user_id,
            iid=movie_id,
        )
        return predict

    def getting_recommend_for_metrics(
            self,
            user_id: int,
            movies_list: List[int],
            top_k: int = 50,
    ) -> List:
        predictions = []
        for iid in tqdm(movies_list, desc=f"Рекомендации для пользователя {user_id}", unit="item"):
            est = self.model.predict(user_id, iid).est
            predictions.append((iid, est))

        predictions.sort(key=lambda x: x[1], reverse=True)
        return [iid for iid, _ in predictions[:top_k]]
    

    def getting_recommended_movies(self, user_id: int, top_k: int = 50):
        """Простая функция рекомендаций: выбирает топ фильмов, не просмотренных пользователем"""
        all_items = set(self.model.trainset.all_items())
        user_items = set([j for (j, _) in self.model.trainset.ur[self.model.trainset.to_inner_uid(user_id)]])
        items_to_predict = list(all_items - user_items)

        # predictions = [
        #     (iid, self.model.predict(user_id, self.model.trainset.to_raw_iid(iid)).est)
        #     for iid in items_to_predict
        # ]

        predictions = []
        for iid in tqdm(items_to_predict, desc=f"Рекомендации для пользователя {user_id}", unit="item"):
            est = self.model.predict(user_id, self.model.trainset.to_raw_iid(iid)).est
            predictions.append((iid, est))

        predictions.sort(key=lambda x: x[1], reverse=True)
        return [iid for iid, _ in predictions[:top_k]]


if __name__ == "__main__":
    train = dd.read_csv('data_csv/output/train/*.csv')
    test = dd.read_csv('data_csv/output/test/*.csv')

    model = MLItemBasedCFSimple()
    model.fit(train)

    sample_user_id = train[settings.data.column_names.userId].head(1).iloc[0]
    recs = model.getting_recommended_movies(sample_user_id, top_k=5)
    print(f"Рекомендации для пользователя {sample_user_id}: {recs}")

