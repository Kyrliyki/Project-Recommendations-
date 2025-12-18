# Тестирование

## Стратегии тестирования 
1. Data Quality tests - Проверка качества входных данных
1. Unit-тесты - Тестирование отдельных функций и модулей
2. Интеграционные тесты - Тестирование взаимодействия различных компонентов/пайплайнов

## Data Quality tests
1. Проверка временных утечек (Train < 2.Validation < Test по timestamp)
2. Целостность и струтура данных:
    - наличие (userId: int, movieId: int, rating: float, timestamp: datetime)
    - Отсутсвие пропусков значений 
    - Отсутсвие дубликатов: 1 фильма = 1 оценка конкретного пользователя
### Запусков тестов
```bash
poetry run pytest tests/quality_of_data -v
```
## Unit-тесты
Цель: покрытие тестами > 70%
- Тесты сплитов данных
- Тесты моделей: безлайны, item-based cf, svd
- Тесты метрик: Precision, Recall, MAP, NDCG
- Тесты пайплайнов: MetricPipeline, SVDPipeline, ItemBasedPipeline, DataLoader и т.д.
### Запус тестов
```bash
poetry run pytest tests/unit -v
```
## Интеграционные тесты
- Тестирование взаимодействия загрузки сырых данных, разделение их на сплиты и последующее их сохранение
- Тестирование взаимодействия разделение данных по сплитам и последующее обучение модели и выдача рекомендаций
- Тестирование взаимодействия модели с протоколами предобработки данных для последующей оценки рекомендаций
- Тестирование протокола предобработки данных для дальшейго использования обработанных данных в метриках оценки рекомендаций
### Запус тестов
```bash
poetry run pytest tests/integration -v
```
## Запуск всех тестов 
```bash
poetry run pytest
```
## Покрытие тестами
- Unit-тесты: 81%
- Интерграционные тесты: основные функции и пайлайны, учавствующие в полном цикле от сырых данных до выдачи метрик


## Continuous Integration
### GitHub Actions Workflow
`.github/workflows/tests.yml:`

```yaml
name: Tests

on:
  workflow_dispatch:
  push:
    branches:
      [main, Work]
  pull_request:
    branches:
      [main, Work]


jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.12]
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}

      - name: Install poetry
        run: |
          curl -sSL https://install.python-poetry.org | python3 -
          echo "$HOME/.local/bin" >> $GITHUB_PATH

      - name: Configure Poetry
        run: poetry config virtualenvs.in-project true


      - name: Load cached Poetry installation
        uses: actions/cache@v3
        with:
          path: ~/.local
          key: poetry-${{ runner.os }}-${{ hashFiles('**/poetry.lock') }}


      - name: Load cached dependencies
        uses: actions/cache@v3
        with:
          path: .venv
          key: venv-${{ runner.os }}-${{ matrix.python-version }}-${{ hashFiles('**/poetry.lock') }}

      - name: Install dependencies
        run: poetry install --with dev

      - name: Cache Kaggle MovieLens dataset
        uses: actions/cache@v3
        id: dataset-cache
        with:
          path: data/raw/
          key: movielens-20m-dataset-v1


      - name: Download Kaggle MovieLens dataset
        if: steps.dataset-cache.outputs.cache-hit != 'true'
        run: |
          mkdir -p data/raw
          wget -q -O data/raw/ml-20m.zip "https://www.kaggle.com/api/v1/datasets/download/grouplens/movielens-20m-dataset"
          cd data/raw && unzip -o ml-20m.zip
          rm -f ml-20m.zip
          echo "Dataset downloaded and extracted"


      - name: Verify dataset files
        run: |
          echo "Checking downloaded files:"
          ls -la data/raw/
          echo "---"
          poetry run python -c "
          import pandas as pd
          import os
          
          required_files = ['movie.csv', 'rating.csv', 'tag.csv', 'link.csv', 'genome_scores.csv', 'genome_tags.csv']
          for file in required_files:
              path = f'data/raw/{file}'
              if os.path.exists(path):
                  df = pd.read_csv(path, nrows=5)
                  print(f'{file}: {len(df.columns)} columns')
              else:
                  print(f'{file} not found')
          "
      - name: Run tests with coverage
        run: poetry run pytest

      - name: Upload test artifacts
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: coverage-reports
          path: |
            htmlcov/
            coverage.xml
          retention-days: 30
```