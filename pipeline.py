from clearml import Task, PipelineDecorator
from clearml.automation import PipelineController

import global_config

# Task.debug_simulate_remote_task(task_id='07839552876c4093aa48a2d6fca00792')


def pre_execute_callback_example(a_pipeline, a_node, current_param_override):
    # type (PipelineController, PipelineController.Node, dict) -> bool
    print('Cloning Task id={} with parameters: {}'.format(a_node.base_task_id, current_param_override))
    # if we want to skip this node (and subtree of this node) we return False
    # return True to continue DAG execution
    return True


def post_execute_callback_example(a_pipeline, a_node):
    # type (PipelineController, PipelineController.Node) -> None
    print('Completed Task id={}'.format(a_node.executed))
    # if we need the actual executed Task: Task.get_task(task_id=a_node.executed)
    return


def compare_metrics_and_publish_best(**kwargs):
    from clearml import OutputModel
    # Compare accuracies from all incoming nodes
    current_best = ('', 0)
    for node_name, training_task_id in kwargs.items():
        accuracy = Task.get_task(task_id=training_task_id).get_reported_scalars()['Accuracy']['Accuracy']['y'][0]
        print(node_name, accuracy)
        if accuracy > current_best[1]:
            current_best = (training_task_id, accuracy)
            print(f"New current best model: {node_name}")

    print(f"Final best model made by step: {current_best[0]}")
    # # Get the best node and tag it as being best
    best_task = Task.get_task(task_id=current_best[0])
    OutputModel(name="best_pipeline_model", base_model_id=best_task.models['output'][0].id)
    # best_task.add_tags(['best_of_run'])
    # # Also upload the best model to the pipeline as a whole, so it's easier to get to from the UI
    # # task will be injected once this function runs as part of the pipeline
    # OutputModel(task=task, base_model_id=best_task.models['output'][0].id)


# Connecting ClearML with the current pipeline,
# from here on everything is logged automatically
pipe = PipelineController(
    name='Workflow Example - Controller Task',
    project=global_config.PROJECT_NAME,
    version='0.0.1'
)

pipe.set_default_execution_queue('default')
pipe.add_parameter('training_seeds', [42, 420, 500])
pipe.add_parameter('query', 'SELECT * FROM df WHERE year <= 2021')

pipe.add_step(
    name='get_data',
    base_task_project=global_config.PROJECT_NAME,
    base_task_name='get data',
    parameter_override={'General/query': '${pipeline.query}'}
)
pipe.add_step(
    name='preprocess_data',
    parents=['get_data'],
    base_task_project=global_config.PROJECT_NAME,
    base_task_name='preprocess data',
    pre_execute_callback=pre_execute_callback_example,
    post_execute_callback=post_execute_callback_example
)
training_nodes = []
# Seeds should be pipeline arguments
# Don't change these when doing new run
for i, random_state in enumerate(pipe.get_parameters()['training_seeds']):
    node_name = f'model_training_{i}'
    training_nodes.append(node_name)
    pipe.add_step(
        name=node_name,
        parents=['preprocess_data'],
        base_task_project=global_config.PROJECT_NAME,
        base_task_name='model training',
        parameter_override={'General/num_boost_round': 250,
                            'General/test_size': 0.5,
                            'General/random_state': random_state}
    )

pipe.add_function_step(
    name='tag_best_model',
    parents=training_nodes,
    function=compare_metrics_and_publish_best,
    function_kwargs={node_name: '${%s.id}' % node_name for node_name in training_nodes},
    monitor_models=["best_pipeline_model"]
)
# pipe.add_step(
#     name='tag_best_model',
#     parents=training_nodes,
#     base_task_project=global_config.PROJECT_NAME,
#     base_task_name='check models',
#     parameter_override={'General/nodes': [(node_name, '${%s.id}' % node_name) for node_name in training_nodes]},
#     monitor_models=['best_pipeline_model']
# )


# for debugging purposes use local jobs
# pipe.start_locally(run_pipeline_steps_locally=True)
# pipe.start_locally()
# Starting the pipeline (in the background)
pipe.start()

print('done')
