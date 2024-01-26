def individual_serial(test) -> dict:
    return {
        "_id": str(test["_id"]),
        "name": test["name"],
        "description": test["description"],
        "complete": test["complete"]
    }

def list_serial(tests) -> list:
    return [individual_serial(test) for test in tests]