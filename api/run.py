from lib.Norway_Automation import handler as scrape_handler

def handler(request):
    """
    Vercel serverless function entrypoint.
    Keep this file tiny and safe.
    """
    try:
        result = scrape_handler(None)
        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": str(result)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Scrape failed: {type(e).__name__}: {e}"
        }
