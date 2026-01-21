# grid.py

# Locked maturity grid (days-to-expiration)
DAYS_GRID = [10, 30, 60, 90, 120, 150, 180, 360, 540, 720]

# Locked delta grid used by vsurfd tables (delta stored in "percent points", e.g. -90, -85, ... +90)
# We include puts negative, calls positive. 50 is "roughly ATM" in delta-space.
DELTA_GRID = [-90, -85, -80, -75, -70, -65, -60, -55, -50, -45, -40, -35, -30, -25, -20, -15, -10,
               10,  15,  20,  25,  30,  35,  40,  45,  50,  55,  60,  65,  70,  75,  80,  85,  90]

CP_FLAGS = ["C", "P"]
