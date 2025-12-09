import traceback
from lib.Norway_Automation import handler as scrape_handler

def handler(request):
    try:
        result = scrape_handler(None)
        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": str(result)
        }
    except Exception as e:
        tb = traceback.format_exc()
        print("SCRAPE CRASHED:\n", tb, flush=True)  # goes to Vercel logs
        return {
            "statusCode": 500,
            "headers": {"content-type": "text/plain"},
            "body": f"Error:\n{e}\n\nTraceback:\n{tb}"
        }
