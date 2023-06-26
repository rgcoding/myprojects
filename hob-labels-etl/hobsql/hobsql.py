sql_lab_results = """SELECT i.id AS inventory_id,Btrim(i.strain) AS inventory_strain,
CASE
 WHEN p.name IS NULL THEN Btrim(i.strain)
 ELSE Btrim(p.name)
END As product_name,
pc.name as product_category,
p.strain AS bt_product_strain,
Date(i.created) as created,
CASE
    WHEN (i.inventorytype = -1) THEN 'Accessories'
    ELSE it.name
END AS inventory_type,
l.name AS location_name,
to_char(to_timestamp(s.sessiontime),'YYYY-MM-DD') AS date_tested,
i.sample_id AS sample_id,
 CASE WHEN s.result = 1 THEN 'Passed' WHEN s.result = -1 THEN 'Failed' ELSE 'Pending' END AS sample_status,cbd AS 
 bt_potency_cbd, thca AS bt_potency_thca, thc AS bt_potency_thc, cbdA AS bt_potency_cbda,qa_total AS bt_potency_total, 
 custom_1 AS Terpenes FROM inventory i JOIN (SELECT x.inventoryparentid,(qa_data-> 'CBD')::numeric AS cbd,
 (qa_data-> 'CBDA')::numeric AS cbda,(qa_data-> 'THC')::numeric AS thc,
 (qa_data-> 'THCA')::numeric AS thca,qa_data-> 'Total' AS qa_total,qa_data,y.sessiontime,y.inventoryid,
 y.result FROM (SELECT sa.inventoryparentid,sa.id,string_agg(CONCAT_WS('=>',r.name,r.value),',')::hstore as qa_data 
 FROM (SELECT MAX(id) AS id,inventoryparentid FROM bmsi_labresults_samples WHERE deleted = 0 GROUP BY 
 inventoryparentid) sa JOIN bmsi_labresults_potency_analysis r ON r.sample_id = sa.id GROUP BY 
 sa.inventoryparentid,sa.id) x JOIN bmsi_labresults_samples y ON y.id = x.id) s ON 
 s.inventoryparentid = i.inventoryparentid LEFT JOIN products p ON i.productid = p.id LEFT JOIN inventoryrooms ir 
 ON ir.id = i.currentroom INNER JOIN locations l ON l.id = i.location LEFT JOIN inventorytypes it 
 ON it.id = i.inventorytype LEFT JOIN productcategories pc ON p.productcategory = pc.id LEFT JOIN vendors v ON 
 p.defaultvendor = v.id  where i.created between '{start_date}' and '{current_date}' """


col_prod_category = ['BRAND', 'BRAND_CODE', 'BT_INVENTORY_ID', 'BT_INVENTORY_STRAIN',
                 'CATEGORY', 'CBD_THC_RATIO', 'COUNT', 'CULTIVATOR', 'EXTRACTION_METHOD',
                 'FLAVOR', 'NAME', 'PRODUCT_CODE', 'PRODUCT_ID', 'PRODUCT_STRAIN_ID',
                 'STRAIN_CODE', 'STRAIN_TYPE', 'SUB_CATEGORY', 'SUB_CATEGORY_01',
                 'TERPENES', 'WEIGHT', 'WEIGHT_EACH', 'WEIGHT_UNIT',
                 'WEIGHT_VALUE', 'INVENTORY_ID', 'PRODUCT_NAME', 'PRODUCT_CATEGORY',
                 'INVENTORY_STRAIN', 'INVENTORY_TYPE', 'BT_POTENCY_CBD',
                 'BT_POTENCY_THCA', 'BT_POTENCY_THC', 'BT_POTENCY_CBDA',
                 'BT_POTENCY_TOTAL', 'LOAD_DT']