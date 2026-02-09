with DAG():
    ztbk_account = Task(
        task_id = "ztbk_account", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "ztbk_account", "sourceName" : "westpac_edw_views", "sourceType" : "Table"}
    )
