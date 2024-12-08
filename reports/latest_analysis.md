Loading data...

Sample of raw dates:
0    2024-12-07 08:07:46
1    2024-12-07 08:00:49
2    2024-12-07 08:00:00
3    2024-12-07 07:21:44
4    2024-12-07 07:10:26
Name: date, dtype: object

Converting dates...

Processed 100 valid incidents

Sample of processed dates:
0   2024-12-07 08:07:46
1   2024-12-07 08:00:49
2   2024-12-07 08:00:00
3   2024-12-07 07:21:44
4   2024-12-07 07:10:26
Name: date, dtype: datetime64[ns]

Analyzing incidents...

=== Recent Crime Analysis ===

NEWSAPI
Total incidents: 100

---
Date: 2024-12-07 08:07
Location: NDTV News
Type: news_report
Description: A 22-year-old man was arrested for allegedly killing his mother after she refused to allow him to marry the woman of his choice and threatened to not give him the property in Delhi's Khayala area.

---
Date: 2024-12-07 08:00
Location: WSB Atlanta
Type: news_report
Description: During the course of their investigation, police learned the man was a victim of an armed robbery.

---
Date: 2024-12-07 08:00
Location: Katinamagazine.org
Type: news_report
Description: By making values-aligned investments in open publishing and infrastructure, academic libraries are helping to create a more equitable knowledge ecosystem

---
Date: 2024-12-07 07:21
Location: The Times of India
Type: news_report
Description: Bansara, a new film directed by Atiul Islam, is set in a Purulia jungle and explores the intertwined themes of mythology, superstition, and crime. The story follows Gaurika Devi, a powerful zamindar descendant played by Aparajita Adhya, and a police officer, …

---
Date: 2024-12-07 07:10
Location: ABC News
Type: news_report
Description: The fatal shooting of UnitedHealthcare's CEO has opened the door for many people to vent their frustrations and anger over the insurance industry

Generating statistics...

=== Crime Statistics ===

Incidents by Source:
source
newsapi    100
Name: count, dtype: int64

Date Range:
Earliest: 2024-12-06 23:53
Latest: 2024-12-07 08:07

Incidents by Time of Day:
hour
Night (12AM-6AM)        78
Morning (6AM-12PM)       6
Evening (6PM-12AM)       1
Afternoon (12PM-6PM)     0
Name: count, dtype: int64

Most Common Crime Types:
crime_type
news_report    100
Name: count, dtype: int64

Analysis generated at: Sun Dec  8 08:27:18 UTC 2024
