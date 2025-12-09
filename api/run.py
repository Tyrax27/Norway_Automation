import json
from Norway_Automation import handler as scrape_handler

def handler(request=None):
    """
    Vercel Serverless Function entrypoint.
    Callable via:
      https://<project>.vercel.app/api/run
    """

    try:
        result = scrape_handler(None)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "ok": True,
                "result": result
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "ok": False,
                "error": str(e)
            })
        }
