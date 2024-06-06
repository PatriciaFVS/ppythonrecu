import pandas as pd
from db import clientPS


def insert_category(category_id, name):
    try: 
        conn= clientPS.client()
        cur = conn.cursor()
        cur.execute(f"""SELECT * FROM category WHERE category_id={category_id};""")
        
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE category
                        SET category_id={category_id}, name='{name}', updated_at= CURRENT_TIMESTAMP
	                    WHERE category_id={category_id};""")
        else:
            cur.execute(f"INSERT INTO category (category_id, name, created_at, updated_at) VALUES ({category_id}, '{name}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        conn.commit()
    except Exception as e:
        print(f'Error conexió {e}')
    finally:
        conn.close()


def insert_subcategory(subcategory_id, name, category_id):
    try:
        conn= clientPS.client()
        cur = conn.cursor()
        cur.execute(f"""SELECT * FROM subcategory WHERE subcategory_id={subcategory_id};""")
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE subcategory
                        SET subcategory_id={subcategory_id}, name='{name}', category_id={category_id}, updated_at=CURRENT_TIMESTAMP
	                    WHERE category_id={category_id};""")
        else:
            cur.execute(f"INSERT INTO subcategory (subcategory_id, name, category_id, created_at, updated_at) VALUES ({subcategory_id}, '{name}', {category_id}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        conn.commit()
    except Exception as e:
        print(f'Error de connexió {e}')
    finally:
        conn.close()



def insert_product(product_id, name, description, company, price, units, subcategory_id):
    try:
        conn= clientPS.client()
        cur = conn.cursor()
       
        cur.execute(f"""SELECT * FROM product WHERE product_id={product_id};""")
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE product
                        SET product_id={product_id}, name='{name}', description='{description}', company='{company}', price={price}, units={units} , subcategory_id={subcategory_id}, updated_at=CURRENT_TIMESTAMP
	                    WHERE product_id={product_id};""")
        else:
            cur.execute(f"INSERT INTO product (product_id, name, description, company, price, units, subcategory_id, created_at, updated_at) VALUES ({product_id}, '{name}', '{description}', '{company}', {price}, {units}, {subcategory_id}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        conn.commit()
    except Exception as e:
        print(f'Error de connexió {e}')
    finally:
        conn.close()




def load(fitxerCSV): 
    try:
        conn = clientPS.client()
        df = pd.read_csv(fitxerCSV.file, header=0 )
        
       
        for index, row in df.iterrows():
            fila= row.to_dict()
            
            
            insert_category(fila["id_categoria"], fila["nom_categoria"])
            insert_subcategory(fila["id_subcategoria"], fila["nom_subcategoria"], fila["id_categoria"])
            insert_product(fila["id_producto"], fila["nom_producto"], fila["descripcion_producto"], fila["companyia"], fila["precio"], fila["unidades"], fila["id_subcategoria"])

        conn.commit()
        return "Pujat correctament"
    except Exception as e:
        
        return "No s'ha pujat correctament"
    finally:
        conn.close()  