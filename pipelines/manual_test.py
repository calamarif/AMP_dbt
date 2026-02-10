with DAG():
    refs_covid_repay_type = Task(
        task_id = "refs_covid_repay_type", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "refs_covid_repay_type", "sourceName" : "westpac_efs", "sourceType" : "Table"}
    )
    refs_repay_type_map = Task(
        task_id = "refs_repay_type_map", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "refs_repay_type_map", "sourceName" : "westpac_efs", "sourceType" : "Table"}
    )
    manual_test__Union_1 = Task(
        task_id = "manual_test__Union_1", 
        component = "Model", 
        modelName = "manual_test__Union_1"
    )
