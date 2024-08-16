from datetime import datetime, timedelta

def DateTime_summary(value, dictionary):
	try:
		date_time = value.GetChildMemberWithName("datetime")
		date = date_time.GetChildMemberWithName("date")
		yof = date.GetChildMemberWithName("yof").GetChildAtIndex(0).GetChildAtIndex(0).GetValue()
		time = date_time.GetChildMemberWithName("time")
		secs = time.GetChildMemberWithName("secs").GetValue()
		year = int(yof) >> 13
		day_of_year = (int(yof) & 8191) >> 4
		date = datetime(year - 1, 12, 31) + timedelta(days=day_of_year) + timedelta(seconds=int(secs))
		return date.strftime("%Y-%m-%d %H:%M:%S")
	except Exception as error:
		print(error)
		return "<error>"