import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

class DataAnalyzer:
    def __init__(self, data_dir='data'):
        self.data_dir = Path(data_dir)
        self.results = {}
    
    def load_data(self):
        """Загрузка ratings и сплитов"""
        ratings_path = self.data_dir / 'raw' / 'rating.csv'
        self.ratings = pd.read_csv(ratings_path)
        

        try:
            self.train = pd.read_parquet(self.data_dir / 'split' / 'train.parquet')
            self.validation = pd.read_parquet(self.data_dir / 'split' / 'validation.parquet')
            self.test = pd.read_parquet(self.data_dir / 'split' / 'test.parquet')
            splits_loaded = True
        except FileNotFoundError:
            print("Сплиты не найдены, анализируем только основной датасет")
            splits_loaded = False
            
        return splits_loaded
    
    def calculate_sparsity(self, df, name):
        """Вычисление sparsity для датафрейма"""
        n_users = df['userId'].nunique()
        n_items = df['movieId'].nunique()
        n_ratings = len(df)
        
        total_possible_ratings = n_users * n_items
        sparsity = (1 - (n_ratings / total_possible_ratings)) * 100
        
        return {
            'dataset': name,
            'n_users': n_users,
            'n_items': n_items,
            'n_ratings': n_ratings,
            'total_possible_ratings': total_possible_ratings,
            'sparsity_percent': sparsity,
            'density_percent': 100 - sparsity
        }
    
    def analyze_movie_ratings_distribution(self, df, name):
        """Анализ распределения количества оценок по фильмам"""
        ratings_per_movie = df['movieId'].value_counts()

        stats = ratings_per_movie.describe()
        value_counts_stats = ratings_per_movie.value_counts().sort_index()

        return {
            'dataset': name,
            'min_ratings_per_movie': stats['min'],
            'max_ratings_per_movie': stats['max'],
            'mean_ratings_per_movie': stats['mean'],
            'median_ratings_per_movie': stats['50%'],
            'std_ratings_per_movie': stats['std'],
            'q1_ratings_per_movie': stats['25%'],
            'q3_ratings_per_movie': stats['75%'],
            'total_movies': len(ratings_per_movie),
            'movies_with_1_rating': value_counts_stats.get(1, 0),
            'movies_with_less_than_5_ratings': (ratings_per_movie < 5).sum(),
            'movies_with_less_than_10_ratings': (ratings_per_movie < 10).sum(),
            'movies_with_more_than_100_ratings': (ratings_per_movie > 100).sum()
        }
    
    def analyze_user_ratings_distribution(self, df, name):
        """Анализ распределения количества оценок по пользователям"""
        ratings_per_user = df['userId'].value_counts()

        stats = ratings_per_user.describe()

        value_counts_stats = ratings_per_user.value_counts().sort_index()
        return {
            'dataset': name,
            'min_ratings_per_user': stats['min'],
            'max_ratings_per_user': stats['max'],
            'mean_ratings_per_user': stats['mean'],
            'median_ratings_per_user': stats['50%'],
            'std_ratings_per_user': stats['std'],
            'q1_ratings_per_user': stats['25%'],
            'q3_ratings_per_user': stats['75%'],
            'total_users': len(ratings_per_user),
            'users_with_1_rating': value_counts_stats.get(1, 0),
            'users_with_less_than_30_ratings': (ratings_per_user < 30).sum(),
            'users_with_less_than_50_ratings': (ratings_per_user < 50).sum(),
            'users_with_more_than_100_ratings': (ratings_per_user > 100).sum()
        }
    

    
    def analyze_dataset(self, df, name):
        """Полный анализ датасета"""
        analysis = {}
        
        # Базовые статистики
        analysis['basic_stats'] = {
            'dataset': name,
            'start_date': df['timestamp'].min() if 'timestamp' in df.columns else 'N/A',
            'end_date': df['timestamp'].max() if 'timestamp' in df.columns else 'N/A',
            'rating_min': df['rating'].min(),
            'rating_max': df['rating'].max(),
            'rating_mean': df['rating'].mean(),
            'rating_std': df['rating'].std()
        }
        
        # Sparsity анализ
        analysis['sparsity'] = self.calculate_sparsity(df, name)
        
        # Анализ фильмов
        analysis['movie_stats'] = self.analyze_movie_ratings_distribution(df, name)
        # Анализ пользователей
        analysis['user_stats'] = self.analyze_user_ratings_distribution(df, name)

        
        return analysis
    
    def generate_comprehensive_report(self):
        """Генерация комплексного отчета"""
        splits_loaded = self.load_data()
        
        # Анализ основного датасета
        self.results['full'] = self.analyze_dataset(self.ratings, 'ratings')
        
        # Анализ сплитов
        if splits_loaded:
            self.results['train'] = self.analyze_dataset(self.train, 'train')
            self.results['validation'] = self.analyze_dataset(self.validation, 'validation')
            self.results['test'] = self.analyze_dataset(self.test, 'test')
        
        return self.results

    def create_visualizations(self, show_plots=False, save_path=None):
        """Создание визуализаций для анализа данных"""

        if save_path is None:
            save_path = self.data_dir / 'analysis'
        else:
            save_path = Path(save_path)

        #save_path.mkdir(parents=True, exist_ok=True)

        movie_stats = self.analyze_movie_ratings_distribution(self.ratings, 'full')
        user_stats = self.analyze_user_ratings_distribution(self.ratings, 'full')


        fig = plt.figure(figsize=(16, 14))


        gs = plt.GridSpec(3, 2, figure=fig, hspace=0.4, wspace=0.3)

        # 1. Распределение оценок
        ax1 = fig.add_subplot(gs[0, 0])
        rating_counts = self.ratings['rating'].value_counts().sort_index()
        ax1.bar(rating_counts.index, rating_counts.values,
                color='skyblue', alpha=0.7, edgecolor='black')
        ax1.set_title('Распределение рейтинга', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Оценка')
        ax1.set_ylabel('Количество')

        ax1.set_xlim(0, 5)
        ax1.set_xticks(np.arange(0, 5.5, 0.5))
        mean_rating = self.ratings['rating'].mean()
        median_rating = self.ratings['rating'].median()
        ax1.axvline(mean_rating, color='blue', linestyle='--',
                    label=f'Среднее: {mean_rating:.2f}')
        ax1.axvline(mean_rating, color='red', linestyle='--',
                    label=f'Медиана: {median_rating:.2f}')
        ax1.legend()
        ax1.grid(True, alpha=0.3)


        ax2 = fig.add_subplot(gs[0, 1])
        ratings_per_user = self.ratings['userId'].value_counts()

        ax2.hist(ratings_per_user, bins=50, edgecolor='black',
                 alpha=0.7, color='lightgreen')
        ax2.set_title('Распределение оценок на пользователя',
                      fontsize=14, fontweight='bold')
        ax2.set_xlabel('Количество оценок на пользователя')
        ax2.set_ylabel('Количество пользователей')
        ax2.set_yscale('log')
        ax2.grid(True, alpha=0.3)


        ax2.axvline(user_stats['median_ratings_per_user'], color='red',
                    linestyle='--', label=f'Медиана: {user_stats["median_ratings_per_user"]:.0f}')
        ax2.axvline(user_stats['mean_ratings_per_user'], color='blue',
                    linestyle='--', label=f'Среднее: {user_stats["mean_ratings_per_user"]:.1f}')
        ax2.legend()

        # 3. Распределение оценок на фильм
        ax3 = fig.add_subplot(gs[1, 0])
        ratings_per_movie = self.ratings['movieId'].value_counts()

        ax3.hist(ratings_per_movie, bins=50, edgecolor='black',
                 alpha=0.7, color='salmon')
        ax3.set_title('Распределение оценок на фильм',
                      fontsize=14, fontweight='bold')
        ax3.set_xlabel('Количество оценок на фильм')
        ax3.set_ylabel('Количество фильмов')
        ax3.set_yscale('log')
        ax3.grid(True, alpha=0.3)


        ax3.axvline(movie_stats['median_ratings_per_movie'], color='red',
                    linestyle='--', label=f'Медиана: {movie_stats["median_ratings_per_movie"]:.0f}')
        ax3.axvline(movie_stats['mean_ratings_per_movie'], color='blue',
                    linestyle='--', label=f'Среднее: {movie_stats["mean_ratings_per_movie"]:.1f}')
        ax3.legend()

        # 4. Временной ряд оценок (если есть timestamp)
        ax4 = fig.add_subplot(gs[1, 1])
        if 'timestamp' in self.ratings.columns:
            self.ratings['date'] = pd.to_datetime(self.ratings['timestamp'])
            monthly_ratings = self.ratings.set_index('date').resample('ME').size()

            ax4.plot(monthly_ratings.index, monthly_ratings.values,
                     linewidth=2, marker='o', markersize=3, color='purple')
            ax4.set_title('Динамика оценок по месяцам', fontsize=14, fontweight='bold')
            ax4.set_xlabel('Дата')
            ax4.set_ylabel('Количество оценок')
            ax4.tick_params(axis='x', rotation=45)
            ax4.grid(True, alpha=0.3)

        else:
            ax4.text(0.5, 0.5, 'Данные о времени отсутствуют',
                     ha='center', va='center', transform=ax4.transAxes, fontsize=12)
            ax4.set_title('Динамика оценок по месяцам', fontsize=14, fontweight='bold')



        # Сохраняем график
        plt.savefig(save_path / 'data_analysis_plots.png', dpi=300, bbox_inches='tight',
                    facecolor='white', edgecolor='none')

        # Показываем график только если явно указано
        if show_plots:
            plt.show()
        else:
            plt.close(fig)

        print(f"Графики сохранены в {save_path / 'data_analysis_plots.png'}")
    
    def save_report(self, output_file='analysis/data_analysis_report.md'):
        """Сохранение отчета в Markdown формате"""
        report = []
        
        report.append("# Отчет по аналитике")
        report.append("## Анализ разреженности и остальная статистика\n")
        

        report.append("### Анализ разреженности")
        report.append("| Dataset | Users | Movies | Ratings | Sparsity | Density |")
        report.append("|---------|-------|--------|---------|----------|---------|")
        
        for dataset_name, analysis in self.results.items():
            sparsity = analysis['sparsity']
            report.append(f"| {sparsity['dataset']} | "
                         f"{sparsity['n_users']:,} | "
                         f"{sparsity['n_items']:,} | "
                         f"{sparsity['n_ratings']:,} | "
                         f"{sparsity['sparsity_percent']:.2f}% | "
                         f"{sparsity['density_percent']:.4f}% |")
        
        # Статистики по фильмам
        report.append("\n### Статистика по фильмам")
        report.append("| Dataset | Min Ratings Count | Max Ratings Count | Mean | Median | Std | Q1 | Q3 |")
        report.append("|---------|-------------------|-------------------|------|--------|-----|----|----|")
        
        for dataset_name, analysis in self.results.items():
            movie_stats = analysis['movie_stats']
            report.append(f"| {movie_stats['dataset']} | "
                          f"{movie_stats['min_ratings_per_movie']} | "
                          f"{movie_stats['max_ratings_per_movie']:,} | "
                          f"{movie_stats['mean_ratings_per_movie']:.1f} | "
                          f"{movie_stats['median_ratings_per_movie']:.1f} | "
                          f"{movie_stats['std_ratings_per_movie']:.1f} | "
                          f"{movie_stats.get('q1_ratings_per_movie', 'N/A')} | "
                          f"{movie_stats.get('q3_ratings_per_movie', 'N/A')} |")
        
        # Статистики по пользователям
        report.append("\n### Статистика по пользователям")
        report.append("| Dataset | Min Ratings Count | Max Ratings Count | Mean | Median | Std | Q1 | Q3 |")
        report.append("|---------|-------------------|-------------------|------|--------|-----|----|----|")
        
        for dataset_name, analysis in self.results.items():
            user_stats = analysis['user_stats']
            report.append(f"| {user_stats['dataset']} | "
                          f"{user_stats['min_ratings_per_user']} | "
                          f"{user_stats['max_ratings_per_user']:,} | "
                          f"{user_stats['mean_ratings_per_user']:.1f} | "
                          f"{user_stats['median_ratings_per_user']:.1f} | "
                          f"{user_stats['std_ratings_per_user']:.1f} | "
                          f"{user_stats.get('q1_ratings_per_user', 'N/A')} | "
                          f"{user_stats.get('q3_ratings_per_user', 'N/A')} |")

        # Распределение оценок
        report.append("\n## Распределение оценок")
        rating_distribution = self.ratings['rating'].value_counts().sort_index()
        report.append("| Оценка | Количество | Процент |")
        report.append("|--------|------------|---------|")
        total_ratings = len(self.ratings)
        for rating, count in rating_distribution.items():
            percentage = (count / total_ratings) * 100
            report.append(f"| {rating} | {count:,} | {percentage:.1f}% |")

        report.append(f"| **Среднее** | **{self.ratings['rating'].mean():.2f}** | - |")
        report.append(f"| **Медиана** | **{self.ratings['rating'].median():.1f}** | - |")

        #Анализ сплитов
        if 'train' in self.results and 'validation' in self.results and 'test' in self.results:
            report.append("\n## Анализ сплитов (Train/Validation/Test)")


            report.append("\n### Сравнение сплитов")
            report.append("| Метрика | Train | Validation | Test | Всего |")
            report.append("|---------|-------|------------|------|-------|")

            total_ratings = self.results['full']['sparsity']['n_ratings']
            total_users = self.results['full']['sparsity']['n_users']
            total_movies = self.results['full']['sparsity']['n_items']

            train_ratings = self.results['train']['sparsity']['n_ratings']
            val_ratings = self.results['validation']['sparsity']['n_ratings']
            test_ratings = self.results['test']['sparsity']['n_ratings']

            train_users = self.results['train']['sparsity']['n_users']
            val_users = self.results['validation']['sparsity']['n_users']
            test_users = self.results['test']['sparsity']['n_users']

            train_movies = self.results['train']['sparsity']['n_items']
            val_movies = self.results['validation']['sparsity']['n_items']
            test_movies = self.results['test']['sparsity']['n_items']

            report.append(
                f"| **Оценки** | {train_ratings:,} ({train_ratings / total_ratings * 100:.1f}%) | {val_ratings:,} ({val_ratings / total_ratings * 100:.1f}%) | {test_ratings:,} ({test_ratings / total_ratings * 100:.1f}%) | {total_ratings:,} |")
            report.append(
                f"| **Пользователи** | {train_users:,} ({train_users / total_users * 100:.1f}%) | {val_users:,} ({val_users / total_users * 100:.1f}%) | {test_users:,} ({test_users / total_users * 100:.1f}%) | {total_users:,} |")
            report.append(
                f"| **Фильмы** | {train_movies:,} ({train_movies / total_movies * 100:.1f}%) | {val_movies:,} ({val_movies / total_movies * 100:.1f}%) | {test_movies:,} ({test_movies / total_movies * 100:.1f}%) | {total_movies:,} |")

            # Анализ пересечений
            report.append("\n### Анализ пересечений пользователей и фильмов")

            # Получаем множества пользователей и фильмов для каждого сплита
            train_users_set = set(self.train['userId'])
            val_users_set = set(self.validation['userId'])
            test_users_set = set(self.test['userId'])

            train_movies_set = set(self.train['movieId'])
            val_movies_set = set(self.validation['movieId'])
            test_movies_set = set(self.test['movieId'])


            users_only_train = len(train_users_set - val_users_set - test_users_set)
            users_only_val = len(val_users_set - train_users_set - test_users_set)
            users_only_test = len(test_users_set - train_users_set - val_users_set)
            users_all_splits = len(train_users_set & val_users_set & test_users_set)


            movies_only_train = len(train_movies_set - val_movies_set - test_movies_set)
            movies_only_val = len(val_movies_set - train_movies_set - test_movies_set)
            movies_only_test = len(test_movies_set - train_movies_set - val_movies_set)
            movies_all_splits = len(train_movies_set & val_movies_set & test_movies_set)

            report.append("#### Пользователи")
            report.append("| Категория | Количество | Процент |")
            report.append("|-----------|------------|---------|")
            report.append(f"| Только в Train | {users_only_train:,} | {users_only_train / total_users * 100:.1f}% |")
            report.append(f"| Только в Validation | {users_only_val:,} | {users_only_val / total_users * 100:.1f}% |")
            report.append(f"| Только в Test | {users_only_test:,} | {users_only_test / total_users * 100:.1f}% |")
            report.append(f"| Во всех сплитах | {users_all_splits:,} | {users_all_splits / total_users * 100:.1f}% |")

            report.append("\n#### Фильмы")
            report.append("| Категория | Количество | Процент |")
            report.append("|-----------|------------|---------|")
            report.append(f"| Только в Train | {movies_only_train:,} | {movies_only_train / total_movies * 100:.1f}% |")
            report.append(
                f"| Только в Validation | {movies_only_val:,} | {movies_only_val / total_movies * 100:.1f}% |")
            report.append(f"| Только в Test | {movies_only_test:,} | {movies_only_test / total_movies * 100:.1f}% |")
            report.append(
                f"| Во всех сплитах | {movies_all_splits:,} | {movies_all_splits / total_movies * 100:.1f}% |")

        # Детальный анализ cold start проблемы
        # report.append("\n### Cold Start Problem Analysis")
        # full_analysis = self.results['full']
        #
        # report.append(f"- **Фильмы с только одной оценкой**: {full_analysis['movie_stats']['movies_with_1_rating']:,} "
        #              f"({full_analysis['movie_stats']['movies_with_1_rating']/full_analysis['sparsity']['n_items']*100:.1f}% от всех фильмов)")
        #
        # report.append(f"- **Пользователи с только одной оценкой**: {full_analysis['user_stats']['users_with_1_rating']:,} "
        #              f"({full_analysis['user_stats']['users_with_1_rating']/full_analysis['sparsity']['n_users']*100:.1f}% от всех пользователей)")
        #
        # report.append(f"- **Фильмы с менее чем 5 оценками**: {full_analysis['movie_stats']['movies_with_less_than_5_ratings']:,} "
        #              f"({full_analysis['movie_stats']['movies_with_less_than_5_ratings']/full_analysis['sparsity']['n_items']*100:.1f}% от всех фильмов)")
        #
        # report.append(f"- **Пользователи с менее чем 5 оценками**: {full_analysis['user_stats']['users_with_less_than_5_ratings']:,} "
        #              f"({full_analysis['user_stats']['users_with_less_than_5_ratings']/full_analysis['sparsity']['n_users']*100:.1f}% от всех пользователей)")
        #
        # # Выводы и рекомендации
        # report.append("\n## Key Insights and Recommendations")
        #
        # full_sparsity = self.results['full']['sparsity']
        # report.append(f"1. **Высокая Sparsity**: {full_sparsity['sparsity_percent']:.2f}% sparsity означает, что "
        #              "рекомендательная система должна эффективно работать с разреженными данными")
        #
        # report.append("2. **Cold Start Challenge**: Значительное количество фильмов и пользователей с малым количеством оценок "
        #              "требует стратегий для обработки cold start проблемы")
        

        # Сохранение отчета
        with open(self.data_dir / output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        print(f"Report saved to {self.data_dir / output_file}")

