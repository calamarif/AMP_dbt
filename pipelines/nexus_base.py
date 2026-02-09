with DAG():
    znex_account = Task(
        task_id = "znex_account", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "znex_account", "sourceName" : "westpac_sgdw_views", "sourceType" : "Table"}
    )
    znex_loan = Task(
        task_id = "znex_loan", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "znex_loan", "sourceName" : "westpac_sgdw_views", "sourceType" : "Table"}
    )
