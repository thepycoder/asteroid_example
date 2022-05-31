from pathlib import Path

import pandas as pd
from clearml import Task, Dataset, StorageManager

import global_config


task = Task.init(
    project_name=global_config.PROJECT_NAME,
    task_name='get data',
    task_type='data_processing',
    reuse_last_task_id=False
)


def log_dataset_statistics(dataset, local_path):
    df = pd.read_csv(local_path)
    dataset.get_logger().report_table(title='Asteroid Data', series='head', table_plot=df.head())


# TODO: change this to StorageManager or similar using database
data_path = Path('data/nasa.csv')

# Create a ClearML dataset
dataset = Dataset.create(
    dataset_name='raw_asteroid_dataset',
    dataset_project=global_config.PROJECT_NAME
)
# Add the local files we downloaded earlier
dataset.add_files(data_path)
# Let's add some cool graphs as statistics in the plots section!
log_dataset_statistics(dataset, data_path)
# Finalize and upload the data and labels of the dataset
dataset.finalize(auto_upload=True)
