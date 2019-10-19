import os

#lin
desktop = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop') 
print(desktop)
#win
desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') 
print(desktop)
print(os.environ['USERPROFILE'])
print(os.environ["HOMEPATH"])

desktop = os.path.expanduser("~/Desktop")
print(desktop)
desktop = os.path.normpath(os.path.expanduser("~/Desktop"))
print(desktop)
