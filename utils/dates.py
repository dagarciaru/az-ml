from datetime import datetime, timedelta

def add_years(date, years_to_add):
    try:
        return date.replace(year= date.year + years_to_add)
    except:
        return date.replace(year= date.year + years_to_add, day= date.day - 1)

def get_year_windows(start_date, last_date, window_years = 1):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end =  datetime.strptime(last_date, '%Y-%m-%d')
    window_end = add_years(start, window_years)
    
    year_windows = []
    while window_end <= end:
        year_windows.append({
            'initDate': start.strftime('%Y-%m-%d'),
            'endDate': window_end.strftime('%Y-%m-%d')
        })
        start = window_end + timedelta(days=1)
        window_end = start.replace(year= start.year + window_years)
    if start <= end:
        year_windows.append({   
            'initDate': start.strftime('%Y-%m-%d'),
            'endDate': last_date if window_end > end else window_end.strftime('%Y-%m-%d')
        })
    return year_windows

