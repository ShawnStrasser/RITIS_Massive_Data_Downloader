# RITIS Massive Data Downloader
A Python class for automated downloading of INRIX XD segment data using the RITIS Massive Data Downloader interface.
Still working on code as of Jan 11, 2023

### What problem does this solve?
Currently, the RITIS API supports downloading data from TMC roadway segments only, it does not support XD segment data. The TMC segments are miles long, whereas the XD segments are a half mile long or less, so they are better suited (but not always ideal) for analysis at the intersection level, such as for traffic signals.

For production use where daily tracking/monitoring is needed, this code automates the data download as part of a data pipeline. Note that once the RITIS API includes XD segment data, this code become obsolete. Also, the INRIX API could  be used but provides lower quality data for some reason.

### How does it work?
The MechanicalSoup library is used to log into RITIS and submit jobs.

Jobs will include all XD segments from a text file segments.txt, which is populated by the user before hand.
The user can specify a date range to be used for a single download, or for automated daily downloads run the update() function
which submits jobs one day at a time until yesterday, starting with the date saved in the last_run.txt file.

### Credits
Special thanks to ChatGPT for writing this code...
