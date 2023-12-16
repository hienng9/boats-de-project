# schema_fields = [
#       {'name': 'subject', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'list_id', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'images', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'list_time', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'list_datetime', 'type': 'DATETIME', 'mode': 'NULLABLE'}, 
#       {'name': 'list_date', 'type': 'DATE', 'mode': 'NULLABLE'}, 
#       {'name': 'list_hour', 'type': 'INT64', 'mode': 'NULLABLE'}, 
#       {'name': 'utc_offset', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'polepos_ad', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'account_account_ads', 'type': 'INT64', 'mode': 'NULLABLE'}, 
#       {'name': 'account_account_id', 'type': 'INT64', 'mode': 'NULLABLE'}, 
#       {'name': 'account_name', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'account_user_id', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'account_eid_verified', 'type': 'BOOL', 'mode': 'NULLABLE'}, 
#       {'name': 'account_created', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'category_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'category_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'list_price_currency', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'list_price_value', 'type': 'INT64', 'mode': 'NULLABLE'}, 
#       {'name': 'location_region_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'location_region_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'location_area_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'location_area_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'location_zipcode_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'location_zipcode_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'location_coordinates_lat', 'type': 'DOUBLE', 'mode': 'NULLABLE'}, 
#       {'name': 'location_coordinates_lng', 'type': 'DOUBLE', 'mode': 'NULLABLE'}, 
#       {'name': 'location_coordinates_is_precise', 'type': 'BOOL', 'mode': 'NULLABLE'}, 
#       {'name': 'type_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'type_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#       {'name': 'ad_details_boat_engine_brand_single_code', 'type': 'STRING', 'mode': 'NULLABLE'},
#       {'name': 'ad_details_boat_engine_brand_single_label', 'type': 'STRING', 'mode': 'NULLABLE'},
#        {'name': 'ad_details_boat_engine_stroke_single_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_boat_engine_stroke_single_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_cx_engine_power_single_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_cx_engine_power_single_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_fuel_single_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_fuel_single_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_general_condition_single_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_general_condition_single_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_regdate_single_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_regdate_single_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': '_app_vehicleBrandId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': '_app_vehicleModelId', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': '_app_price', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': '_app_title', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': '_app_url', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': '_app_urlAbsolute', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': '_app_subtitle', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'store_details_id', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'store_details_name', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'store_details_has_contacts', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_cx_used_hour_single_code', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'ad_details_cx_used_hour_single_label', 'type': 'STRING', 'mode': 'NULLABLE'}, 
#        {'name': 'EngineHp', 'type': 'STRING', 'mode': 'NULLABLE'}
#        ]


schema = '(subject STRING , list_id STRING , images STRING , list_time STRING , list_datetime INT64 , list_date DATE , list_hour INT64 , utc_offset STRING , status STRING , polepos_ad STRING , account_account_ads INT64 , account_account_id INT64 , account_name STRING , account_user_id STRING , account_eid_verified BOOL , account_created STRING , category_code STRING , category_label STRING , list_price_currency STRING , list_price_value INT64 , location_region_code STRING , location_region_label STRING , location_area_code STRING , location_area_label STRING , location_zipcode_code STRING , location_zipcode_label STRING , location_coordinates_lat FLOAT64 , location_coordinates_lng FLOAT64 , location_coordinates_is_precise BOOL , type_code STRING , type_label STRING , ad_details_boat_engine_brand_single_code STRING , ad_details_boat_engine_brand_single_label STRING , ad_details_boat_engine_stroke_single_code STRING , ad_details_boat_engine_stroke_single_label STRING , ad_details_cx_engine_power_single_code STRING , ad_details_cx_engine_power_single_label STRING , ad_details_fuel_single_code STRING , ad_details_fuel_single_label STRING , ad_details_general_condition_single_code STRING , ad_details_general_condition_single_label STRING , ad_details_regdate_single_code STRING , ad_details_regdate_single_label STRING , _app_vehicleBrandId STRING , _app_vehicleModelId STRING , _app_price STRING , _app_title STRING , _app_url STRING , _app_urlAbsolute STRING , _app_subtitle STRING , store_details_id STRING , store_details_name STRING , store_details_has_contacts BOOL , ad_details_cx_used_hour_single_code STRING , ad_details_cx_used_hour_single_label STRING , EngineHp STRING)'