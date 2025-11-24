import os
from typing import List

import pandas as pd
import re


def update_directly(
        markdown_table_metrics,
        markdown_table_accuracy,
        path='docs/EVALUATION.md'
):
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
        f'<!-- METRICS_TABLE -->\n{markdown_table_metrics}\n<!-- METRICS_TABLE -->',
        content,
        flags=re.DOTALL
    )
    updated_content = re.sub(
        r'<!-- METRICS_ACCURACY_TABLE -->.*<!-- METRICS_ACCURACY_TABLE -->',
        f'<!-- METRICS_ACCURACY_TABLE -->\n{markdown_table_accuracy}\n<!-- METRICS_ACCURACY_TABLE -->',
        updated_content,
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


def concat_files(
        folder_path: str,
        files_to_merge: List[str],
):
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
        return None

    # Объединяем
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df


def main():
    folder_path = 'data/models/'
    files_to_merge = [
        'svd_metrics.csv',
        'svd_v2_metrics.csv',
        'svd_v3_metrics.csv',
        'ibcf_v2_metrics.csv',
        'ibcf_v3_metrics.csv',
    ]

    files_to_merge_accuracy =[
        'svd_accuracy_metrics.csv',
    ]

    result_df = concat_files(
        folder_path=folder_path,
        files_to_merge=files_to_merge,
    )
    result_accuracy_df = concat_files(
        folder_path=folder_path,
        files_to_merge=files_to_merge_accuracy,
    )

    # Сортируем столбцы: сначала мета, потом метрики
    meta_cols = ['model', 'evaluation_protocol', 'source_file']

    metric_cols = [col for col in result_df.columns if col not in meta_cols]
    result_df = result_df[meta_cols + metric_cols]

    metric_accuracy_cols = [col for col in result_accuracy_df.columns if col not in meta_cols]
    result_accuracy_df = result_accuracy_df[meta_cols + metric_accuracy_cols]

    # Сохраняем
    output_csv = 'data/models/result_metrics_for_all_models.csv'
    result_df.to_csv(output_csv, index=False)
    print(f"Результат сохранён: {output_csv}")

    output_accuracy_csv = 'data/models/result_accuracy_for_all_models.csv'
    result_accuracy_df.to_csv(output_accuracy_csv, index=False)
    print(f"Результат сохранён: {output_accuracy_csv}")

    # Markdown
    markdown_table_metrics = result_df.to_markdown(index=False)
    markdown_table_accuracy = result_accuracy_df.to_markdown(index=False)
    md_path = 'data/models/result_metrics_for_all_models.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("## Метрики моделей\n\n")
        f.write(markdown_table_metrics)
        f.write("\n\n## Точность моделей относительно предсказанных оценок\n\n")
        f.write(markdown_table_accuracy)
    print(f"Markdown сохранён: {md_path}")

    # Обновляем docs/EVALUATION.md
    update_directly(markdown_table_metrics, markdown_table_accuracy)


if __name__ == "__main__":
    main()