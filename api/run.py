import os
from lib.Norway_Automation import handler as scrape_handler

def handler(request):
    """
    Vercel serverless entrypoint.
    Visit /api/run to execute.
    """

    # (Optional) shared-secret protection if you ever want it later
    secret = os.environ.get("CRON_SECRET")
    provided = request.headers.get("x-vercel-cron-secret")
    if secret and provided != secret:
        return {"statusCode": 401, "body": "unauthorized"}

    try:
        result = scrape_handler(None)
        return {"statusCode": 200, "body": str(result)}
    except Exception as e:
        # surface real error in logs + response
        return {"statusCode": 500, "body": f"Error: {e}"}
