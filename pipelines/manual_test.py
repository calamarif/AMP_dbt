with DAG():
    manual_test__repay_type_source_data = Task(
        task_id = "manual_test__repay_type_source_data", 
        component = "Model", 
        modelName = "manual_test__repay_type_source_data"
    )
