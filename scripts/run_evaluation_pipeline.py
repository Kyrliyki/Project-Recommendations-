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
        print("Доступные .md файлы в текущей директории:")
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


def infer_model_and_protocol(filename: str):
    # Приводим к нижнему регистру для надёжности
    name = filename.lower()

    # Определяем модель
    if 'svd' in name:
        model = 'SVD'
    elif 'ibcf' in name or 'item' in name:
        model = 'ItemCF'
    else:
        model = 'Unknown'

    # Определяем протокол
    if '_v2' in name:
        protocol = '1:99 per positive (leave-one-out)'
    elif '_v3' in name:
        protocol = 'aggregated (all positives + 99 neg. each)'
    elif 'metrics.csv' == name or name.endswith('_metrics.csv'):
        # базовый файл без суффикса — предположим исходный протокол (например, по validation-only)
        protocol = 'validation-only ranking'
    else:
        protocol = 'unknown'

    return model, protocol


def main():
    folder_path = 'data/models/'
    files_to_merge = [
        'svd_metrics.csv',
        'svd_v2_metrics.csv',
        'svd_v3_metrics.csv',
        'ibcf_v2_metrics.csv',
        'ibcf_v3_metrics.csv',
    ]

    dfs = []
    for file in files_to_merge:
        full_path = os.path.join(folder_path, file)
        if not os.path.exists(full_path):
            print(f"Пропускаем отсутствующий файл: {full_path}")
            continue

        # Читаем как pandas, т.к. файлы небольшие
        df = pd.read_csv(full_path)
        model, protocol = infer_model_and_protocol(file)

        df['model'] = model
        df['evaluation_protocol'] = protocol
        df['source_file'] = file

        dfs.append(df)

    if not dfs:
        print("Нет данных для объединения.")
        return

    # Объединяем
    result_df = pd.concat(dfs, ignore_index=True)

    # Сортируем столбцы: сначала мета, потом метрики
    meta_cols = ['model', 'evaluation_protocol', 'source_file']
    metric_cols = [col for col in result_df.columns if col not in meta_cols]
    result_df = result_df[meta_cols + metric_cols]

    # Сохраняем
    output_csv = 'data/models/result_metrics_for_all_models.csv'
    result_df.to_csv(output_csv, index=False)
    print(f"Результат сохранён: {output_csv}")

    # Markdown
    markdown_table = result_df.to_markdown(index=False)
    md_path = 'data/models/result_metrics_for_all_models.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# Метрики моделей\n\n")
        f.write(markdown_table)
    print(f"Markdown сохранён: {md_path}")

    # Обновляем docs/EVALUATION.md
    update_directly(markdown_table)


if __name__ == "__main__":
    main()