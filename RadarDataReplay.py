import datetime as dt
import shutil
import time


def main():
  
  # Configuration options for the scans:
  scheduleInterval = 6 # Minutes
  
  
  # Define data types (moments):
  dataTypesBasic = ["dBZ", "V"]
  dataTypesSingle = ["dBuZ", "dBZ", "V", "W"]
  dataTypesDual = ["dBuZ", "dBZ", "V", "W", "SNR", "ZDR", "uPhiDP", "RhoHV", "PhiDP", "KDP"]
  
  #Define scan parameters:
  aziLongRange = AzimuthScan("LongRange-DP", 0.5, 18, dataTypesSingle)

  volStandardScan = VolumeScan("Standard6min-DP", [])
  volStandardScan.addSweep(AzimuthScan("tilt01", 0.5, 12, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt02", 0.9, 12, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt03", 1.3, 18, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt04", 1.8, 24, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt05", 2.4, 24, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt06", 3.1, 24, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt07", 4.2, 24, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt08", 5.6, 24, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt09", 7.4, 24, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt10", 10.0, 32, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt11", 13.3, 32, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt12", 17.9, 32, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt13", 23.9, 32, dataTypesDual))
  volStandardScan.addSweep(AzimuthScan("tilt14", 32.0, 32, dataTypesDual))
  
  aziBirdBath = AzimuthScan("BirdBath", 90, 32, dataTypesDual)
  
  #Define task list:
  scheduledTasks = [aziLongRange, volStandardScan, aziBirdBath]
  
  
  # Run the schedule:
  scheduleIntervalSec = 60 * scheduleInterval
  while True:
    t = dt.datetime.utcnow()
    tSeconds = 3600*t.hour + 60*t.minute + t.second
    lagNextSchedule = scheduleIntervalSec - (tSeconds % scheduleIntervalSec)
    
    print("Waiting for next schedule (%d s)" % lagNextSchedule)
    
    time.sleep(lagNextSchedule)
        
    for task in scheduledTasks:
      task.run()  
  
  
  
  

# Define an azimuth scan (sweep)
class AzimuthScan:
  def __init__(self, name, elev, speed, moments):
    self.name = name
    self.elev = elev
    self.speed = speed
    self.moments = moments
  
  
  # Simulate the radar scan
  def run(self):
    time.sleep(3) # Simulate antenna move time
    scanTime = 360.0 / self.speed
    
    scanStartTime = dt.datetime.utcnow()
    
    print("  Commencing azimuth scan %s at %s" % (self.name, scanStartTime.strftime("%Y-%m-%d %H:%M:%S")))
    
    time.sleep(scanTime)
    
    # Prepare the files for transfer
    for moment in self.moments:
      fileName = "AUS1_%s.azi_%s%s.azi" % (self.name, scanStartTime.strftime("%Y%m%d%H%M%S"), moment)
      print("    Processing file %s" % fileName)
      shutil.copyfile("data/dummy.azi","data/forTransfer/%s" % fileName)
    
    print("  Completed azimuth scan %s at %s" % (self.name, dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
    








#Define a volume scan (collection of sweeps)
class VolumeScan:
  def __init__(self, name, sweeps):
    self.name = name
    self.sweeps = sweeps

  def addSweep(self, sweep):
    sweep.name = "%s_%s" % (self.name, sweep.name)
    self.sweeps.append(sweep)
    
  def run(self):
    #print(" Commencing volume scan %s at %s" % (self.name, dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
    for sweep in self.sweeps:
      sweep.run()
    #print(" Completed volume scan %s at %s" % (self.name, dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))


main()