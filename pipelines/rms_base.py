with DAG():
    zrms_loan_account = Task(
        task_id = "zrms_loan_account", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "zrms_loan_account", "sourceName" : "westpac_edw_views", "sourceType" : "Table"}
    )
    zrms_account = Task(
        task_id = "zrms_account", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "zrms_account", "sourceName" : "westpac_edw_views", "sourceType" : "Table"}
    )
