import os
from Norway_Automation import handler as norway_handler  # api/Norway_Automation.py

def handler(request):
    # Optional security check for cron
    secret = os.environ.get("CRON_SECRET")
    provided = request.headers.get("x-vercel-cron-secret")
    if secret and provided != secret:
        return {
            "statusCode": 401,
            "body": "unauthorized"
        }

    result = norway_handler(None)
    return {
        "statusCode": 200,
        "body": str(result)
    }
