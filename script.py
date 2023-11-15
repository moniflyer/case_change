import psycopg2

def generate_etl(source_schema, source_table):

    try:
        
        connection = psycopg2.connect(
            host="127.0.0.1",
            user="ilya",
            password="123456",
            database="postgres"
        )
    
        connection.autocommit = True
        
        with connection.cursor() as cursor:
                mapping_list = "select * from meta.attribute_mapping"
                mapping_out_table = f"""select distinct target_schema, target_table 
                                    from meta.attribute_mapping 
                                    where source_schema = '{source_schema}' and source_table = '{source_table}'"""
                cursor.execute(mapping_out_table)
                name_schema_table = cursor.fetchall()
                name_schema = name_schema_table[0][0]
                name_table = name_schema_table[0][1]
                cursor.execute(mapping_list)
                res = cursor.fetchall()
    
                name_scheme_table = f"{name_schema}.{name_table}"
                my_query = 'insert into ' + name_scheme_table + '\nselect\n '
                for elem in res:
                    my_query = my_query + elem[6] + ' as ' + elem[5] + ',\n'
                my_query = my_query[:-2]
                my_query = my_query + '\n from source.my_sample'
                cursor.execute(my_query)
                
                my_file = open('case_change_script/select_file.txt', 'w+')
                my_file.write(my_query)
                my_file.close    
                
    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL:", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

generate_etl('source', 'my_sample')