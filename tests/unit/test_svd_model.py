import numpy as np
import pytest
from sklearn.metrics import mean_absolute_error, mean_squared_error
from surprise import Prediction


@pytest.mark.test_svd()
def test_svd_model_fit_predict(trained_svd_model):
    model = trained_svd_model['model']
    test_data = trained_svd_model['split_data']['test']

    assert hasattr(model.model, 'trainset')
    assert model.model.trainset.n_users > 0
    assert model.model.trainset.n_items > 0

    trainset = model.model.trainset

    # Берем существующего пользователя и фильм из обучающей выборки
    user_id = trainset.to_raw_uid(0)
    movie_id = trainset.to_raw_iid(0)

    prediction = model.predict(user_id, movie_id)

    assert isinstance(prediction, Prediction)
    assert hasattr(prediction, 'est')
    assert 0.5 <= prediction.est <= 5

    new_user_id = 99999
    movie_id = model.model.trainset.to_raw_iid(0)

    prediction = model.predict(new_user_id, movie_id)
    assert isinstance(prediction, Prediction)

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

    max_allowed_gap = 0.1  # 10% максимальная разница

    assert rmse_gap_ratio < max_allowed_gap, \
        f"Переобучение по RMSE: разница {rmse_gap_ratio:.1%} > {max_allowed_gap:.0%}"

    assert mae_gap_ratio < max_allowed_gap, \
        f"Переобучение по MAE: разница {mae_gap_ratio:.1%} > {max_allowed_gap:.0%}"

    # Дополнительно: validation не должно быть сильно хуже train
    assert val_rmse < train_rmse * 1.5, \
        f"Validation RMSE ({val_rmse:.3f}) слишком хуже train RMSE ({train_rmse:.3f})"

