from clearml import OutputModel, Task
import global_config


task = Task.init(
    project_name=global_config.PROJECT_NAME,
    task_name='check models',
    task_type='data_processing',
    reuse_last_task_id=False
)

config = {
    'nodes': [('test_node', 'e16a8c3dcfa84355afad6ab096c96966')]
}
task.connect(config)

# Compare accuracies from all incoming nodes
current_best = ('', 0)
for node_name, training_task_id in config['nodes']:
    accuracy = Task.get_task(task_id=training_task_id).get_reported_scalars()['Accuracy']['Accuracy']['y'][0]
    print(node_name, accuracy)
    if accuracy > current_best[1]:
        current_best = (training_task_id, accuracy)
        print(f"New current best model: {node_name}")

print(f"Final best model made by step: {current_best[0]}")
if current_best[0]:
    # # Get the best node and tag it as being best
    best_task = Task.get_task(task_id=current_best[0])
    OutputModel(name="best_pipeline_model", base_model_id=best_task.models['output'][0].id)
