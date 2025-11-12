import os
import dask.dataframe as dd
import pandas as pd
import re
from datetime import datetime


def update_directly(markdown_table, path='docs/EVALUATION.md'):

    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"Файл найден: {path}")
    except FileNotFoundError:
        print(f"Файл не найден по пути: {path}")
        print("Доступные файлы в текущей директории:")
        for file in os.listdir('.'):
            if file.lower().endswith('.md'):
                print(f"  - {file}")
        return

    updated_content = re.sub(
        r'<!-- METRICS_TABLE -->.*<!-- METRICS_TABLE -->',
        f'<!-- METRICS_TABLE -->\n{markdown_table}\n<!-- METRICS_TABLE -->',
        content,
        flags=re.DOTALL
    )

    with open(path, 'w', encoding='utf-8') as f:
        f.write(updated_content)


def main():
    folder_path = 'data/models/'
    files_to_merge = [
        'svd_metrics.csv',
        'svd_v2_metrics.csv',
        'svd_v3_metrics.csv'
    ]
    full_paths = [os.path.join(folder_path, file) for file in files_to_merge]

    df = dd.read_csv(full_paths)
    df_computed = df.compute()

    df_computed.to_csv('data/models/result_metrics_for_all_models.csv', index=False)

    markdown_table = df_computed.to_markdown(index=False)
    with open('data/models/result_metrics_for_all_models.md', 'w', encoding='utf-8') as f:
        f.write(f"# Метрики моделей\n\n{markdown_table}")

    update_directly(markdown_table)

    print(f"Объединены файлы: {files_to_merge}")


if __name__ == "__main__":
    main()