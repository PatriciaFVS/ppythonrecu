from pydantic import BaseModel

class Product(BaseModel):
    id:int
    name:str
    description: str
    company:str
    price:float
    unit:int
    subcategory_id:int
