import psycopg2
from config import host, user, password, db_name


try:
    
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )

    connection.autocommit = True
    
    with connection.cursor() as cursor:
            mapping_list = "select * from meta.attribute_mapping"
            mapping_out_table = """select distinct target_schema, target_table 
                                from meta.attribute_mapping 
                                where source_schema = 'source' and source_table = 'my_sample'"""
            cursor.execute(mapping_out_table)
            name_schema_table = cursor.fetchall()
            name_schema = name_schema_table[0][0]
            name_table = name_schema_table[0][1]
            cursor.execute(mapping_list)
            res = cursor.fetchall()

            def case_change(name_schema, name_table):
                name_scheme_table = f"{name_schema}.{name_table}"
                my_query = 'insert into ' + name_scheme_table + '\nselect\n '
                for elem in res:
                    my_query = my_query + elem[6] + ' as ' + elem[5] + ',\n'
                my_query = my_query[:-2]
                my_query = my_query + '\n from source.my_sample'
                cursor.execute(my_query)

            case_change(name_schema, name_table)
        

            
except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL:", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")