import os
from Norway_Automation import handler  # or from api.Norway_Automation import handler

def main(request):
    # Optional security check for cron
    secret = os.environ.get("CRON_SECRET")
    provided = request.headers.get("x-vercel-cron-secret")
    if secret and provided != secret:
        return {
            "statusCode": 401,
            "body": "unauthorized"
        }

    result = handler(None)
    return {
        "statusCode": 200,
        "body": str(result)
    }
