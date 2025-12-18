import numpy as np
import pytest
from sklearn.metrics import mean_absolute_error, mean_squared_error
from surprise import Prediction


@pytest.mark.test_svd()
def test_svd_model_fit_predict(trained_svd_model):
    model = trained_svd_model['model']

    assert hasattr(model.model, 'trainset')
    assert model.model.trainset.n_users > 0
    assert model.model.trainset.n_items > 0

    trainset = model.model.trainset

    user_id = trainset.to_raw_uid(0)
    movie_id = trainset.to_raw_iid(0)

    prediction = model.predict(user_id, movie_id)

    assert isinstance(prediction, Prediction)
    assert hasattr(prediction, 'est')
    assert 0.5 <= prediction.est <= 5

    unknown_user_id = 99999


    prediction = model.predict(unknown_user_id, movie_id)
    assert isinstance(prediction, Prediction)
    assert 0.5 <= prediction.est <= 5.0

    unknown_item_id = 999999

    prediction = model.predict(user_id, unknown_item_id)
    assert isinstance(prediction, Prediction)
    assert 0.5 <= prediction.est <= 5.0


@pytest.mark.overfitting
def test_overfitting_gap(trained_svd_model):
    model = trained_svd_model['model']
    train_data = trained_svd_model['split_data']['train'].compute()
    val_data = trained_svd_model['split_data']['validation'].compute()

    train_predictions = []
    train_actuals = []

    for _, row in train_data.iterrows():
        try:
            pred = model.predict(str(row['userId']), str(row['movieId']))
            train_predictions.append(pred.est)
            train_actuals.append(row['rating'])
        except:
            continue

    val_predictions = []
    val_actuals = []


    for _, row in val_data.iterrows():
        try:
            pred = model.predict(str(row['userId']), str(row['movieId']))
            val_predictions.append(pred.est)
            val_actuals.append(row['rating'])
        except:
            continue


    train_rmse = np.sqrt(mean_squared_error(train_actuals, train_predictions))
    val_rmse = np.sqrt(mean_squared_error(val_actuals, val_predictions))

    train_mae = mean_absolute_error(train_actuals, train_predictions)
    val_mae = mean_absolute_error(val_actuals, val_predictions)

    print(f"Train RMSE: {train_rmse:.3f}, Validation RMSE: {val_rmse:.3f}")
    print(f"Train MAE: {train_mae:.3f}, Validation MAE: {val_mae:.3f}")

    rmse_gap_ratio = abs(train_rmse - val_rmse) / (train_rmse + 1e-10)
    mae_gap_ratio = abs(train_mae - val_mae) / (train_mae + 1e-10)

    max_allowed_gap = 0.1

    assert rmse_gap_ratio < max_allowed_gap, \
        f"Переобучение по RMSE: разница {rmse_gap_ratio:.1%} > {max_allowed_gap:.0%}"

    assert mae_gap_ratio < max_allowed_gap, \
        f"Переобучение по MAE: разница {mae_gap_ratio:.1%} > {max_allowed_gap:.0%}"


    assert val_rmse < train_rmse * 1.5, \
        f"Validation RMSE ({val_rmse:.3f}) слишком хуже train RMSE ({train_rmse:.3f})"

def test_getting_recommended_movies_top_k(trained_svd_model):
    model = trained_svd_model["model"]

    trainset = model.model.trainset
    user_id = trainset.to_raw_uid(0)

    recs = model.getting_recommended_movies(
        user_id=user_id,
        top_k=10
    )

    assert isinstance(recs, list)
    assert len(recs) == 10
    assert all(isinstance(i, int) for i in recs)


def test_getting_recommended_movies_with_candidates(trained_svd_model):
    model = trained_svd_model["model"]
    trainset = model.model.trainset

    user_id = trainset.to_raw_uid(0)

    candidate_items = [
        trainset.to_raw_iid(i)
        for i in range(min(15, trainset.n_items))
    ]

    recs = model.getting_recommended_movies(
        user_id=user_id,
        movies_list=candidate_items,
        top_k=5
    )

    assert len(recs) == 5
    assert set(recs).issubset(set(candidate_items))


def test_recommendations_are_deterministic(trained_svd_model):
    model = trained_svd_model["model"]
    trainset = model.model.trainset
    user_id = trainset.to_raw_uid(0)

    rec_1 = model.getting_recommended_movies(user_id=user_id, top_k=10)
    rec_2 = model.getting_recommended_movies(user_id=user_id, top_k=10)

    assert rec_1 == rec_2


def test_trainset_size_matches_train_data(trained_svd_model):
    model = trained_svd_model["model"]
    train_df = trained_svd_model["split_data"]["train"].compute()

    trainset = model.model.trainset

    assert trainset.n_users == train_df["userId"].nunique()
    assert trainset.n_items == train_df["movieId"].nunique()

