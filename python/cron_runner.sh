export PYTHONPATH=/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10:/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/lib-dyn2load:/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages
cd /Users/feini/dreamhouse/python
/usr/local/bin/python3 get_zillow_data.py --cities 'Fremont,Newark,Union City' --status ForSale --mode advanced --upload
/usr/local/bin/python3 forsale_summary.py

