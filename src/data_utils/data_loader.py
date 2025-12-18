import logging
from pathlib import Path
from typing import Tuple, Optional, Dict, Any, Union
from datetime import datetime
import dask.dataframe as dd
from src.utils.policy import SplitPolicy, DownloadPolicy

from src.data_utils.preparing_data import download_csv
from src.data_utils.split_strategies import SplitStrategy, get_split_strategy

logger = logging.getLogger(__name__)
# logging.StreamHandler()
logger.setLevel(logging.INFO)

class DataLoader:
    """Работа с сырыми данными и сплитами"""

    def __init__(
            self,
            raw_csv_path: Union[str, Path],
            splits_dir: Union[str, Path],

            download_policy: DownloadPolicy = DownloadPolicy.IF_MISSING,
            split_policy: SplitPolicy = SplitPolicy.REUSE,

            split_strategy: Union[str, SplitStrategy] = "time",
            split_strategy_kwargs: Optional[Dict] = None,
            download_url: Optional[str] = None,
    ):

        self.raw_csv_path = Path(raw_csv_path)
        self.splits_dir = Path(splits_dir)

        self.download_policy = download_policy
        self.split_policy = split_policy
        self.download_url = download_url
        self.timestamp_col = 'timestamp'

        if isinstance(split_strategy, str):
            self.split_strategy = get_split_strategy(
                split_strategy,
                **(split_strategy_kwargs or {})
            )
        else:
            self.split_strategy = split_strategy

        self.raw_csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.raw_csv_path = self.raw_csv_path / "rating.csv"
        self.strategy_dir = self.splits_dir / self.split_strategy.get_name()
        self.strategy_dir.mkdir(parents=True, exist_ok=True)


        self.train_path = self.strategy_dir / "train.parquet"
        self.val_path = self.strategy_dir / "val.parquet"
        self.test_path = self.strategy_dir / "test.parquet"


    def load(self) -> Tuple[dd.DataFrame, dd.DataFrame, dd.DataFrame]:
        self._log("Запуск DataLoader pipeline")
        self._ensure_raw_data()
        self._ensure_splits()
        return self._load_splits()


    def _ensure_raw_data(self):
        if self.raw_csv_path.exists():
            if self.download_policy == DownloadPolicy.ALWAYS:
                self._log("CSV найден, но будет перезагружен")
                self._download()
            else:
                self._log("CSV найден, используем существующий")
            return

        if self.download_policy == DownloadPolicy.NEVER:
            raise FileNotFoundError(f"CSV не найден: {self.raw_csv_path}")

        self._log("CSV не найден, скачиваем")
        self._download()

    def _download(self):
        if not self.download_url:
            raise ValueError("download_url обязателен для скачивания")

        start = datetime.now()

        download_csv(
            input_folder_path=self.raw_csv_path.parent,
            url=self.download_url,
        )

        elapsed = (datetime.now() - start).total_seconds()
        self._log(f"Датасет скачан за {elapsed:.1f} сек")


    def _ensure_splits(self):
        splits_exist = all([
            self.train_path.exists(),
            self.val_path.exists(),
            self.test_path.exists(),
        ])

        if splits_exist and self.split_policy == SplitPolicy.REUSE:
            self._log("Parquet сплиты найдены, используем их")
            return

        self._log("Создаём parquet сплиты")
        self._create_and_save_splits()

    def _create_and_save_splits(self):
        self._log("Загружаем CSV")
        df = dd.read_csv(
            self.raw_csv_path,
            parse_dates=[self.timestamp_col],
        )

        self._log("Разделяем данные")
        train, val, test = self.split_strategy.split(df)

        self._log("Сохраняем parquet файлы")
        train.to_parquet(self.train_path, overwrite=True)
        val.to_parquet(self.val_path, overwrite=True)
        test.to_parquet(self.test_path, overwrite=True)



    def _load_splits(self):
        self._log("Загружаем parquet сплиты")

        train = dd.read_parquet(self.train_path)
        val = dd.read_parquet(self.val_path)
        test = dd.read_parquet(self.test_path)

        self._log(
            f"Готово: "
            f"train={len(train):,}, "
            f"val={len(val):,}, "
            f"test={len(test):,}"
        )

        return train, val, test



    def _log(self, message: str, level: str = "INFO"):
        """Логирование"""


        if level == "INFO":
            logger.info(message)
            print(f"[INFO] {message}")
        elif level == "WARNING":
            logger.warning(message)
            print(f"[WARNING] {message}")
        elif level == "ERROR":
            logger.error(message)
            print(f"[ERROR] {message}")

