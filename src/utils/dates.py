from datetime import datetime, timedelta

def today_str(format="%Y-%m-%d"):
    """Return current date as string"""
    return datetime.today().strftime(format)

def yesterday_str(format="%Y-%m-%d"):
    """Return yesterday's date as string"""
    return (datetime.today() - timedelta(days=1)).strftime(format)

def parse_date(date_str, format="%Y-%m-%d"):
    """Parse string to datetime object"""
    return datetime.strptime(date_str, format)