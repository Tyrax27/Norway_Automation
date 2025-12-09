from lib.Norway_Automation import handler as scrape_handler

def handler(request):
    result = scrape_handler(None)
    return {
        "statusCode": 200,
        "body": str(result)
    }
