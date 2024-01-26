from fastapi import APIRouter
from models.test import Test
from config.database import collection_name
from schema.schemas import list_serial
from bson import ObjectId
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from fastapi.templating import Jinja2Templates

router = APIRouter()
STATIC_DIR = Path(__file__).resolve().parent.parent / 'static'
router.mount("/static", StaticFiles(directory="static",html = True), name="static")

templates = Jinja2Templates(directory="fastApp")

@router.get("/")
async def read_root():
    return RedirectResponse(url="/dashboard")

@router.get("/dashboard")
async def dashboard():
    return templates.TemplateResponse("dashboard.html")

# GET request
@router.get("/find")
async def get_tests():
    tests = list_serial(collection_name.find())
    return tests

# POST request
@router.post("/")
async def post_test(test: Test):
    collection_name.insert_one(dict(test))

# PUT request
@router.put("/{id}")
async def put_test(id:str, test:Test):
    collection_name.find_one_and_update({"_id": ObjectId(id)},{"$set":dict(test)})

# DELETE request
@router.delete("/{id}")
async def delete_test(id:str):
    collection_name.find_one_and_delete({"_id": ObjectId(id)})