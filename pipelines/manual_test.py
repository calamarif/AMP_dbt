with DAG():
    refs_covid_repay_type = Task(
        task_id = "refs_covid_repay_type", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "refs_covid_repay_type", "sourceName" : "westpac_efs", "sourceType" : "Table"}
    )
    manual_test__loan_repayment_type_logic = Task(
        task_id = "manual_test__loan_repayment_type_logic", 
        component = "Model", 
        modelName = "manual_test__loan_repayment_type_logic"
    )
