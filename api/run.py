import sys
import os
import traceback

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

# Import at the function level to avoid module-level execution
def handler(request):
    try:
        # Import here instead of at module level
        from Norway_Automation import handler as scrape_handler
        
        result = scrape_handler(request)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": str(result)
        }
    except Exception as e:
        tb = traceback.format_exc()
        print("SCRAPE CRASHED:\n", tb, flush=True)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "text/plain"},
            "body": f"Error:\n{e}\n\nTraceback:\n{tb}"
        }