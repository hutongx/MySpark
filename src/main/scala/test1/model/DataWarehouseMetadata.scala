package test1.model

object DataWarehouseMetadata {

    // ODS Layer Metadata
    val odsCustomer = TableMetadata(
        tableName = "ods_customer",
        database = "ods_db",
        partitionCols = Seq("dt"),
        primaryKeys = Seq("customer_id"),
        businessKeys = Seq("customer_code")
    )

    val odsOrder = TableMetadata(
        tableName = "ods_order",
        database = "ods_db",
        partitionCols = Seq("dt"),
        primaryKeys = Seq("order_id"),
        businessKeys = Seq("order_no")
    )

    // DWD Layer Metadata
    val dwdCustomerDim = TableMetadata(
        tableName = "dwd_dim_customer",
        database = "dwd_db",
        partitionCols = Seq("dt"),
        primaryKeys = Seq("customer_sk"),
        businessKeys = Seq("customer_id")
    )

    val dwdOrderFact = TableMetadata(
        tableName = "dwd_fact_order",
        database = "dwd_db",
        partitionCols = Seq("dt"),
        primaryKeys = Seq("order_sk"),
        businessKeys = Seq("order_id")
    )

    // DWS Layer Metadata
    val dwsCustomerSummary = TableMetadata(
        tableName = "dws_customer_summary",
        database = "dws_db",
        partitionCols = Seq("dt"),
        primaryKeys = Seq("customer_id", "stat_date"),
        businessKeys = Seq("customer_id")
    )
}
