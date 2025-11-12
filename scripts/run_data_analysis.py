from src.data_analyzer import DataAnalyzer


def main():
    analyzer = DataAnalyzer(data_dir='data')
    results = analyzer.generate_comprehensive_report()
    analyzer.create_visualizations()
    analyzer.save_report()


    print("\n=== Анализ данных ===")
    full_stats = results['full']['sparsity']
    print(f"Full dataset sparsity: {full_stats['sparsity_percent']:.2f}%")
    print(f"Total users: {full_stats['n_users']:,}")
    print(f"Total movies: {full_stats['n_items']:,}")
    print(f"Total ratings: {full_stats['n_ratings']:,}")

    movie_stats = results['full']['movie_stats']


if __name__ == "__main__":
    main()