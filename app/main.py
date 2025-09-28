# main.py
# --- IMPORTS ---
import asyncio
import sqlite3
import math
import random
from datetime import datetime, timedelta
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse, StreamingResponse # Import FileResponse & StreamingResponse
from pydantic import BaseModel
import uvicorn
import os
from typing import List, Dict
import io

# --- DATABASE SETUP ---
DB_FILE = "energy_data.db"

def init_db():
    """Initializes the SQLite database and creates the table if it doesn't exist."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE) # Clean slate for each run during development
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE energy_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            solar_power_kw REAL NOT NULL,
            wind_power_kw REAL NOT NULL,
            campus_load_kw REAL NOT NULL,
            battery_level_kwh REAL NOT NULL,
            grid_power_kw REAL NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    print("Database initialized.")

# --- SIMULATION CONFIGURATION ---
MAX_SOLAR_KW = 250.0
MAX_WIND_KW = 80.0
BATTERY_CAPACITY_KWH = 500.0
BATTERY_MAX_CHARGE_KW = 80.0
BATTERY_MAX_DISCHARGE_KW = 80.0

# --- SIMULATION STATE (IN-MEMORY) ---
simulation_state = {
    "battery_level_kwh": BATTERY_CAPACITY_KWH * 0.5, # Start at 50%
    "current_wind_speed": 0.6 # NEW: Add a starting wind speed (e.g., 60%)
}

# --- SIMULATION LOGIC ---
def simulate_solar_power(current_time):
    hour = current_time.hour + current_time.minute / 60.0
    
    # Base solar curve (stronger peak, wider distribution)
    if 5.0 <= hour <= 19.0:
        sun_angle = ((hour - 5.0) / (19.0 - 5.0)) * math.pi
        base_power = MAX_SOLAR_KW * math.sin(sun_angle)
        
        # Add more significant random fluctuations
        fluctuation = random.uniform(-0.15, 0.15) * base_power
        return max(0, base_power + fluctuation)
    return 0.0

# Replace the old simulate_wind_power function with this new one
def simulate_wind_power():
    """
    Simulates wind power with a stateful, gradual change (random walk).
    """
    # 1. Get the last known wind speed from our simulation state
    last_speed = simulation_state["current_wind_speed"]
    
    # 2. Determine a small, random change for this second
    change = random.uniform(-0.15, 0.15) # The speed can change by a max of 2% per second
    
    # 3. Apply the change to get the new speed
    new_speed = last_speed + change
    
    # 4. Make sure the speed stays within realistic bounds (e.g., 10% to 100%)
    if new_speed > 1.0:
        new_speed = 1.0
    elif new_speed < 0.2:
        new_speed = 0.2
        
    # 5. Update the global state with our new wind speed for the next second's calculation
    simulation_state["current_wind_speed"] = new_speed
    
    # 6. Calculate power based on this new, gradually-changed speed
    # Using a simple power curve (power is proportional to the cube of speed)
    power = MAX_WIND_KW * (new_speed ** 3)
    
    # Add a tiny bit of random fluctuation to the final output
    fluctuation = power * random.uniform(-0.05, 0.05)
    
    return max(0, power + fluctuation)

def simulate_campus_load(current_time):
    hour = current_time.hour
    minute = current_time.minute
    
    # Base load curve with more distinct peaks and valleys
    if 0 <= hour < 6: base_load = 60 + random.uniform(-10, 10) # Night
    elif 6 <= hour < 9: base_load = 180 + random.uniform(-30, 30) # Morning rush
    elif 9 <= hour < 12: base_load = 220 + random.uniform(-40, 40) # Day peak 1
    elif 12 <= hour < 14: base_load = 190 + random.uniform(-20, 20) # Lunch dip
    elif 14 <= hour < 17: base_load = 230 + random.uniform(-40, 40) # Day peak 2
    elif 17 <= hour < 22: base_load = 260 + random.uniform(-50, 50) # Evening peak
    else: base_load = 100 + random.uniform(-20, 20) # Late night
    
    # Add minute-level fluctuations for real-time feel
    minute_fluctuation = math.sin(minute / 60.0 * 2 * math.pi) * 20 * random.uniform(0.5, 1.5)
    
    return max(30, base_load + minute_fluctuation) # Ensure minimum load

def run_energy_balance_logic(solar, wind, load, battery_level):
    total_generation = solar + wind
    net_power = total_generation - load
    grid_power_kw = 0
    
    time_step_seconds = 1
    time_step_hours = time_step_seconds / 3600.0

    # Calculate current battery charge/discharge rate
    current_charge_rate = 0
    current_discharge_rate = 0

    if net_power > 0: # Surplus
        charge_power = min(net_power, BATTERY_MAX_CHARGE_KW)
        energy_to_add = charge_power * time_step_hours
        available_capacity = BATTERY_CAPACITY_KWH - battery_level
        
        if available_capacity > 0.1: # Only charge if there's significant capacity
            actual_energy_added = min(energy_to_add, available_capacity)
            battery_level += actual_energy_added
            current_charge_rate = actual_energy_added / time_step_hours
        
        # If battery is full or cannot charge further, export to grid
        if (available_capacity <= 0.1) or (net_power > current_charge_rate):
             grid_power_kw = (net_power - current_charge_rate) # Export surplus to grid
             grid_power_kw = max(0, grid_power_kw) # Ensure it's not negative

    elif net_power < 0: # Deficit
        power_needed = abs(net_power)
        
        if battery_level > 0.1: # Only discharge if there's significant energy
            discharge_power = min(power_needed, BATTERY_MAX_DISCHARGE_KW)
            energy_to_draw = discharge_power * time_step_hours
            actual_energy_drawn = min(energy_to_draw, battery_level)
            battery_level -= actual_energy_drawn
            current_discharge_rate = actual_energy_drawn / time_step_hours
            power_needed -= (actual_energy_drawn / time_step_hours) # Remaining power needed after battery discharge

        if power_needed > 0.1: # If deficit still exists after battery discharge, import from grid
            grid_power_kw = -power_needed # Negative means importing from grid
            
    return max(0, min(BATTERY_CAPACITY_KWH, battery_level)), grid_power_kw

# --- BACKGROUND SIMULATOR TASK ---
async def simulator_task():
    """A background task that generates and stores data every second."""
    print("Starting background simulator task...")
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    
    while True:
        now = datetime.now()
        
        solar = simulate_solar_power(now)
        wind = simulate_wind_power()
        load = simulate_campus_load(now)
        
        new_battery_level, grid = run_energy_balance_logic(
            solar, wind, load, simulation_state["battery_level_kwh"]
        )
        simulation_state["battery_level_kwh"] = new_battery_level

        cursor.execute("""
            INSERT INTO energy_metrics (timestamp, solar_power_kw, wind_power_kw, campus_load_kw, battery_level_kwh, grid_power_kw)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (now, solar, wind, load, new_battery_level, grid))
        conn.commit()
        
        await asyncio.sleep(1)

# --- FASTAPI APP SETUP ---
app = FastAPI(title="Energy Management System API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API MODELS ---
class EnergySnapshot(BaseModel):
    timestamp: datetime
    solar_power_kw: float
    wind_power_kw: float
    campus_load_kw: float
    battery_level_kwh: float
    battery_percentage: float
    grid_power_kw: float
    renewable_generation_kw: float
    is_charging: bool

class Recommendation(BaseModel):
    priority: str
    tag: str
    message: str
    color_code: str # For frontend styling

# --- API ENDPOINTS ---
@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(simulator_task())

@app.get("/api/latest", response_model=EnergySnapshot)
async def get_latest_data():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM energy_metrics ORDER BY timestamp DESC LIMIT 1")
    latest_row = cursor.fetchone()
    conn.close()

    if latest_row:
        battery_percentage = (latest_row["battery_level_kwh"] / BATTERY_CAPACITY_KWH) * 100
        renewable_gen = latest_row["solar_power_kw"] + latest_row["wind_power_kw"]
        
        # Determine if charging or discharging
        # If total generation > load AND battery is not full (charging)
        # If total generation < load AND battery is not empty (discharging)
        # If total generation == load, then standby
        is_charging = False
        if renewable_gen > latest_row["campus_load_kw"] and battery_percentage < 99.5:
            is_charging = True
        elif renewable_gen < latest_row["campus_load_kw"] and battery_percentage > 0.5:
            is_charging = False # Discharging or drawing from grid
        
        return {
            "timestamp": latest_row["timestamp"], "solar_power_kw": round(latest_row["solar_power_kw"], 2),
            "wind_power_kw": round(latest_row["wind_power_kw"], 2), "campus_load_kw": round(latest_row["campus_load_kw"], 2),
            "battery_level_kwh": round(latest_row["battery_level_kwh"], 2), "battery_percentage": round(battery_percentage, 2),
            "grid_power_kw": round(latest_row["grid_power_kw"], 2), "renewable_generation_kw": round(renewable_gen, 2),
            "is_charging": is_charging
        }
    return {}

@app.get("/api/history")
async def get_historical_data(minutes: int = 30):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    time_threshold = datetime.now() - timedelta(minutes=minutes)
    cursor.execute("SELECT * FROM energy_metrics WHERE timestamp >= ? ORDER BY timestamp ASC", (time_threshold,))
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "timestamps": [row['timestamp'] for row in rows], "solar_power_kw": [round(row['solar_power_kw'], 2) for row in rows],
        "wind_power_kw": [round(row['wind_power_kw'], 2) for row in rows], "campus_load_kw": [round(row['campus_load_kw'], 2) for row in rows],
        "battery_percentage": [round((row['battery_level_kwh'] / BATTERY_CAPACITY_KWH) * 100, 2) for row in rows],
    }

@app.get("/api/stats")
async def get_summary_stats():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    time_threshold = datetime.now() - timedelta(hours=24)
    cursor.execute("SELECT * FROM energy_metrics WHERE timestamp >= ?", (time_threshold,))
    rows = cursor.fetchall()
    conn.close()

    if not rows: return {"renewable_usage_percentage": 0, "total_grid_kwh": 0, "carbon_avoided_kg": 0}

    total_load_kwh = sum(row['campus_load_kw'] for row in rows) / 3600
    total_grid_import_kwh = sum(max(0, -row['grid_power_kw']) for row in rows) / 3600 # Only sum positive grid_power_kw (import)
    total_grid_export_kwh = sum(max(0, row['grid_power_kw']) for row in rows) / 3600 # Only sum negative grid_power_kw (export)

    # Renewable usage percentage calculation
    # Renewable Generation - (Load served by Battery + Load served by Grid Import) / Total Load
    total_renewable_generation_kwh = sum(row['solar_power_kw'] + row['wind_power_kw'] for row in rows) / 3600
    renewable_usage_percentage = (total_renewable_generation_kwh - total_grid_export_kwh) / total_load_kwh * 100 if total_load_kwh > 0 else 0
    renewable_usage_percentage = max(0, min(100, renewable_usage_percentage)) # Clamp between 0 and 100

    # Carbon avoided: assuming average grid intensity of 0.82 kg CO2/kWh
    # (Total Load served by renewables and battery) * 0.82
    carbon_avoided_kg = (total_load_kwh - total_grid_import_kwh) * 0.82

    return {
        "renewable_usage_percentage": round(renewable_usage_percentage, 2),
        "total_grid_kwh_24h": round(total_grid_import_kwh, 2), # Now explicitly grid import
        "carbon_avoided_kg_24h": round(carbon_avoided_kg, 2),
    }

# --- NEW: Rule-based Optimization ---
@app.get("/api/recommendations", response_model=List[Recommendation])
async def get_recommendations():
    """Provides smart recommendations based on the latest system state."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM energy_metrics ORDER BY timestamp DESC LIMIT 1")
    latest_row = cursor.fetchone()
    conn.close()
    
    recommendations = []
    
    if not latest_row:
        recommendations.append(Recommendation(
            priority="LOW PRIORITY",
            tag="monitor",
            message="No live data available. System is in standby or initialization phase.",
            color_code="blue"
        ))
        return recommendations

    solar_power = latest_row["solar_power_kw"]
    wind_power = latest_row["wind_power_kw"]
    campus_load = latest_row["campus_load_kw"]
    battery_level_kwh = latest_row["battery_level_kwh"]
    grid_power_kw = latest_row["grid_power_kw"]
    battery_percentage = (battery_level_kwh / BATTERY_CAPACITY_KWH) * 100

    total_generation = solar_power + wind_power
    net_balance = total_generation - campus_load # Positive for surplus, negative for deficit

    # Rule 1: Solar surplus, recommend charging or load shifting
    if solar_power > (campus_load * 1.1) and battery_percentage < 90: # Significant solar surplus
        recommendations.append(Recommendation(
            priority="MEDIUM PRIORITY",
            tag="optimize",
            message="Solar generation is at peak efficiency. Consider increasing campus load during these hours to maximize renewable usage.",
            color_code="orange"
        ))
    elif solar_power > campus_load and battery_percentage >= 90: # Solar surplus but battery almost full
        recommendations.append(Recommendation(
            priority="HIGH PRIORITY",
            tag="export",
            message="Solar generation is high and battery is nearly full. Recommend exporting surplus power to the grid.",
            color_code="green"
        ))
    
    # Rule 2: Campus demand > solar + wind, recommend discharging or grid import
    if campus_load > (total_generation * 1.1) and battery_percentage > 20: # Significant deficit, battery available
        recommendations.append(Recommendation(
            priority="MEDIUM PRIORITY",
            tag="optimize",
            message="High campus demand exceeding renewable supply. Recommend discharging battery to support the load and reduce grid dependency.",
            color_code="blue"
        ))
    elif campus_load > (total_generation * 1.2) and battery_percentage <= 20: # High deficit, low battery
        recommendations.append(Recommendation(
            priority="HIGH PRIORITY",
            tag="import",
            message="Critical energy deficit. Campus demand significantly exceeds generation and battery is low. Prioritize grid import.",
            color_code="red"
        ))

    # Rule 3: Battery level
    if battery_percentage > 95:
        recommendations.append(Recommendation(
            priority="LOW PRIORITY",
            tag="monitor",
            message="Battery level is high. Consider optimizing discharge or preparing for export if generation is also high.",
            color_code="green"
        ))
    elif battery_percentage < 15:
        recommendations.append(Recommendation(
            priority="HIGH PRIORITY",
            tag="charge",
            message="Battery level critically low. Prioritize charging from renewables or grid if necessary to ensure stability.",
            color_code="red"
        ))

    # Rule 4: Grid interaction
    if grid_power_kw < -50: # Significant grid import
        recommendations.append(Recommendation(
            priority="HIGH PRIORITY",
            tag="action",
            message=f"Significant energy import from grid ({abs(grid_power_kw):.1f} kW). Investigate demand reduction or generation increase.",
            color_code="red"
        ))
    elif grid_power_kw > 50: # Significant grid export
        recommendations.append(Recommendation(
            priority="LOW PRIORITY",
            tag="monitor",
            message=f"Exporting surplus power to grid ({grid_power_kw:.1f} kW). System is efficiently utilizing excess renewable energy.",
            color_code="green"
        ))

    if not recommendations:
        recommendations.append(Recommendation(
            priority="LOW PRIORITY",
            tag="monitor",
            message="System operating under normal parameters. No immediate actions required.",
            color_code="blue"
        ))
        
    return recommendations

# --- NEW: Alerts & Reports ---
@app.get("/api/report/csv")
async def get_csv_report():
    """Generates and returns a CSV report of the last 24 hours of data."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    time_threshold = datetime.now() - timedelta(hours=24)
    cursor.execute("SELECT * FROM energy_metrics WHERE timestamp >= ? ORDER BY timestamp ASC", (time_threshold,))
    rows = cursor.fetchall()
    conn.close()

    output = io.StringIO()
    header = [
        "timestamp", "solar_power_kw", "wind_power_kw", 
        "campus_load_kw", "battery_level_kwh", "grid_power_kw"
    ]
    output.write(','.join(header) + '\n')
    
    for row in rows:
        data_row = [
            row["timestamp"],
            f"{row['solar_power_kw']:.2f}",
            f"{row['wind_power_kw']:.2f}",
            f"{row['campus_load_kw']:.2f}",
            f"{row['battery_level_kwh']:.2f}",
            f"{row['grid_power_kw']:.2f}",
        ]
        output.write(','.join(map(str, data_row)) + '\n')

    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=energy_report_{datetime.now().strftime('%Y-%m-%d')}.csv"}
    )

# --- NEW: SERVE FRONTEND ---
@app.get("/")
async def serve_index():
    """Serves the index.html file for the frontend dashboard."""
    return FileResponse('static/index.html', media_type='text/html')

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("Starting FastAPI server...")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)