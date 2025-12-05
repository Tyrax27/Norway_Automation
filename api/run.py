import os
from Norway_Automation import handler as scrape_handler

def handler(request):
    # Optional security check for cron
    secret = os.environ.get("CRON_SECRET")
    provided = request.headers.get("x-vercel-cron-secret")
    if secret and provided != secret:
        return {
            "statusCode": 401,
            "body": "unauthorized"
        }

    result = scrape_handler(None)
    return {
        "statusCode": 200,
        "body": str(result)
    }
