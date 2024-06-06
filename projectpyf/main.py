from typing import Union

from fastapi import FastAPI, UploadFile

from model.product import Product

from db import clientPS
from db import productDB
from db import insertCSV

from model import product


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/product/")
def getproduct():
    data=productDB.consulta()
    return (data) 


@app.get("/product/{id}")
def getproductById(id:int):
    dataId=productDB.consultaId(id)
    return (dataId)


@app.post("/product/")
def createProduct(prod: product.Product):
    return productDB.insert(prod)


@app.put("/product/")
def updatebyId(prod: product.Product):
    dataActualitza=productDB.update(prod) 
    return (dataActualitza)

@app.delete("/product/{id}")
def deletebyId(id:int):
    dataElimina=productDB.delete(id)
    return (dataElimina)

@app.get("/productAll")
def getAll():
    data=productDB.getAll()
    return (data)

@app.post("/uploadfile/")
def create_upload_file(file: UploadFile):
    data= insertCSV.load(file)
    return data