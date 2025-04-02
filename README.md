# pwa-iwt-wave-tour-stats

# ideas
website comments and frothometer button

##### PROJECT NOTES & Roadmap #####
### Trying to outline some steps ###

 - Get historical data as csv
    - PWA
    - IWT (live heats)
 - Setup DB tables and import in 
 - highlight in Lie Heats events list whihc don't have 'heat structure' i.e need PWA
 - work on fuzzy event name match 
 - Process for adding new data?
 - Look at what is needed for report
 - build out database view
 - new report pages 
 - new report design

# Misc
- if event name LIVE - update all event data? two run evey 8 hours?

# Process to add new data
1. Daily check based on 'daily event check script'
2. If status = 'Live','On Hold', or 'has changed since last update' then:
    a. get event_id, division_id, get results
3. If this event already in DB replace with new results
4. If event not in DB add it in


# Report Improvement

- compare best and average counting scores (jumps and waves) to fleet average for whole fleet, top 10, finalists
- update rank formatting in the table on results page so doesnt break when sorted  (dynamic formatting)


# Long Term Projects
- replace powerbi with website
- add chat bot to ask questions