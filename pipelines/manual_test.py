with DAG():
    manual_test__repay_type_source_data = Task(
        task_id = "manual_test__repay_type_source_data", 
        component = "Model", 
        modelName = "manual_test__repay_type_source_data"
    )
    manual_test__repayment_type_extraction = Task(
        task_id = "manual_test__repayment_type_extraction", 
        component = "Model", 
        modelName = "manual_test__repayment_type_extraction"
    )
    manual_test__tbk_base = Task(
        task_id = "manual_test__tbk_base", 
        component = "Model", 
        modelName = "manual_test__tbk_base"
    )
