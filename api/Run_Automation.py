from lib import Norway_Automation as na

def handler(request):
    try:
        result = na.run_scrape(None)
        return {
            "statusCode": 200,
            "body": str(result)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {repr(e)}"
        }
