from Norway_Automation import handler as scrape_handler

def handler(request):
    try:
        result = scrape_handler(None)
        return {
            "statusCode": 200,
            "body": str(result)
        }
    except Exception as e:
        # Helpful failure response for Vercel logs + quick debugging
        return {
            "statusCode": 500,
            "body": f"Error running scrape: {str(e)}"
        }
