with DAG():
    rms_base__repayment_type_extraction = Task(
        task_id = "rms_base__repayment_type_extraction", 
        component = "Model", 
        modelName = "rms_base__repayment_type_extraction"
    )
