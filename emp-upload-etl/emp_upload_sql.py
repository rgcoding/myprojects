#Update new hires & reinstatements
update_users_sql_1 = """update                               
                            users u                         
                        set                                 
                            is_employee = true,             
                            employee_status = 'pending',    
                            employee_suspended_by = null,   
                            employee_suspended_on = null,   
                            updated = current_timestamp,    
                            employee_location = (select distinct location from users_emp_temp uet where  
                            uet.phone = u.phone)            
                        where                               
                            exists (                        
                            select                          
                            1                               
                            from                            
                                ordering_apps oa            
                            where                           
                                u.ordering_app_id = oa.id   
                                and oa.hidden <> true       
                                and oa.deleted is null)     
                            and exists (                    
                            select                          
                                1                           
                            from                            
                                users_emp_temp uet          
                        where                               
                                uet.phone = u.phone)        
                            and u.is_employee = false;"""    

#Update terminated/suspended employee status
update_users_sql_2 = """update                                               
                            users u                                         
                        set                                                 
                            is_employee = false,                            
                            employee_status = 'suspended',                  
                            employee_suspended_by = 8,                      
                            employee_suspended_on = current_timestamp,      
                            updated = current_timestamp                     
                        where                                               
                            exists (                                        
                            select                                          
                                1                                           
                            from                                            
                                ordering_apps oa                            
                            where                                           
                                u.ordering_app_id = oa.id                   
                                and oa.hidden <> true                       
                                and oa.deleted is null)                     
                            and not exists (                                
                            select                                          
                                1                                           
                            from                                            
                                users_emp_temp uet                          
                            where                                           
                                uet.phone = u.phone)                        
                            and u.is_employee = true;"""

sql_get_employee = """SELECT payroll_name as name, home_department_description as dept,location_description as location, union_code, personal_contact_personal_mobile as phone, current_timestamp as last_extract FROM {schema}.EMPLOYEE \
                  where _file = (select distinct _file from {schema}.EMPLOYEE where _modified = (select (max(_modified)) from {schema}.EMPLOYEE));"""  

sql_del_employee = """delete from {schema}.EMPLOYEE where _file <> (select distinct _file from {schema}.EMPLOYEE where _modified = (select (max(_modified)) from {schema}.EMPLOYEE))"""