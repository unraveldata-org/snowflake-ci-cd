SELECT
        c.c_name as "Customer Name",
        o.O_ORDERSTATUS as "Order Status"
    FROM
        ORDERS as o
    INNER JOIN CUSTOMER as c ORDER BY c.c_name LIMIT 10;

select WR_ORDER_NUMBER,WR_ITEM_SK from sfsalesshared_sfc_samples_va3_sample_data.tpcds_sf100tcl.web_returns where wr_fee >= 0.22 order by WR_ORDER_NUMBER,WR_ITEM_SK ;

select WR_ORDER_NUMBER,WR_ITEM_SK from sfsalesshared_sfc_samples_va3_sample_data.tpcds_sf100tcl.web_returns where wr_fee >= 0.22 limit 100;
