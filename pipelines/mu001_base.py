with DAG():
    zwgd_all_acct_m_hist = Task(
        task_id = "zwgd_all_acct_m_hist", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "zwgd_all_acct_m_hist", "sourceName" : "westpac_edw_views", "sourceType" : "Table"}
    )
    zwgd_mrtg_loan_m_hist = Task(
        task_id = "zwgd_mrtg_loan_m_hist", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "zwgd_mrtg_loan_m_hist", "sourceName" : "westpac_edw_views", "sourceType" : "Table"}
    )
