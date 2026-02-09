with DAG():
    zsgd_acct = Task(
        task_id = "zsgd_acct", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "zsgd_acct", "sourceName" : "westpac_edw_views", "sourceType" : "Table"}
    )
    zsgd_acct_loan = Task(
        task_id = "zsgd_acct_loan", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "zsgd_acct_loan", "sourceName" : "westpac_edw_views", "sourceType" : "Table"}
    )
