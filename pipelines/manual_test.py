with DAG():
    manual_test__loan_repayment_type_mapping = Task(
        task_id = "manual_test__loan_repayment_type_mapping", 
        component = "Model", 
        modelName = "manual_test__loan_repayment_type_mapping"
    )
