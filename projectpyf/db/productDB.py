from db import clientPS

def productSchema(prod) ->dict:
    return {"id": str(prod[0]),
            "name":prod[1],
            "description":prod[2],
            "company":prod[3],
            "price": str(prod[4]),
            "units": str(prod[5]),
            "subcategory_id": prod[6],
            "created_at": prod[7],
            "updated_at": prod[8]
        
    }
    
def productSubcategorySchema(prod) ->dict:
    return {
        "categoria": prod[0],
        "subcategoria": prod[1],
        "nom_producte": prod[2],
        "marca_producte": prod[3],
        "preu": str(prod[4])

    }
    
def consulta():
    try:
        conn=clientPS.client()
        
        cur=conn.cursor()
        
        cur.execute("select * from public.product")
        
        data= cur.fetchone()
    except Exception as e:
        print(f'Error connexió {e}')
    
    finally:
        conn.close
        return f"consulta {productSchema(data)}"

def consultaId(id:int):
    try: 
        conn=clientPS.client()
        
        cur=conn.cursor()
        
        cur.execute(f"select * from public.product where product_id={id}")
        
        data=cur.fetchone()
        
    except Exception as e:
        print (f'Error de connexió {e}')
    finally:
        conn.close
        return f"consulta {productSchema(data)}"

def insert(prod):
    try:
        conn=clientPS.client()
        
        cur=conn.cursor()
        
        cur.execute(f"""
                INSERT INTO public.product (product_id, name, description, company, price, units, subcategory_id, created_at, updated_at)
                VALUES ({prod.id},'{prod.name}','{prod.description}','{prod.company}',{prod.price},{prod.unit}, {prod.subcategory_id}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP )
            """)
        
        conn.commit()
        
    except Exception as e:
        print(f'Error connexió {e}')
    
    finally:
        conn.close
        
    return {"message": "S'ha insertat correctament"}

def update(prod):
    try:
        conn=clientPS.client()
        
        cur=conn.cursor()
        
        
        cur.execute(f"UPDATE public.product SET  name='{prod.name}', description='{prod.description}', company='{prod.company}', price={prod.price}, units={prod.unit}, subcategory_id={prod.subcategory_id}, updated_at= CURRENT_TIMESTAMP WHERE product_id = {prod.id}")
        
        conn.commit()
        
    except Exception as e:
        print(f'Error connexió {e}')
    
    finally:
        conn.close
        return {"message": "S'ha insertat correctament"}

def delete(id):
    try:
        conn=clientPS.client()
        
        cur=conn.cursor()
        
        
        cur.execute(f"DELETE FROM public.product WHERE product_id={id};")
        
        conn.commit()
        
    except Exception as e:
        print(f'Error connexió {e}')
    
    finally:
        conn.close
        return {"message": "S'ha eliminat correctament"}


def getAll():
    
    try:
        conn=clientPS.client()
        
        cur=conn.cursor()
        
        cur.execute("""SELECT 
                    c.name AS categoria,
                    s.name AS subcategoria,
                    p.name AS nom_producte,
                    p.company AS marca_producte,
                    p.price AS preu
                    FROM product p
                    JOIN 
                    subcategory s ON p.subcategory_id = s.subcategory_id
                    JOIN 
                    category c ON s.category_id = c.category_id;""")
        
        data= cur.fetchall()
        return data
    except Exception as e:
        print(f'Error connexió {e}')
    
    finally:
        conn.close