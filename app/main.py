# main.py
# --- IMPORTS ---
import asyncio
import sqlite3
import math
import random
from datetime import datetime, timedelta
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse # Import FileResponse
from pydantic import BaseModel
import uvicorn
import os

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
    "battery_level_kwh": BATTERY_CAPACITY_KWH * 0.5 # Start at 50%
}

# --- SIMULATION LOGIC ---
def simulate_solar_power(current_time):
    hour = current_time.hour + current_time.minute / 60.0
    if 5.5 <= hour <= 18.5:
        sun_angle = ((hour - 5.5) / (18.5 - 5.5)) * math.pi
        power = MAX_SOLAR_KW * math.sin(sun_angle)
        power *= (1 + random.uniform(-0.05, 0.05))
        return max(0, power)
    return 0.0

def simulate_wind_power():
    base_wind = MAX_WIND_KW * random.uniform(0.2, 0.9)
    fluctuation = base_wind * random.uniform(-0.15, 0.15)
    return max(0, base_wind + fluctuation)

def simulate_campus_load(current_time):
    hour = current_time.hour
    if 0 <= hour < 6: base_load = 60
    elif 6 <= hour < 9: base_load = 180
    elif 9 <= hour < 17: base_load = 220
    elif 17 <= hour < 22: base_load = 250
    else: base_load = 100
    return base_load * (1 + random.uniform(-0.05, 0.05))

def run_energy_balance_logic(solar, wind, load, battery_level):
    total_generation = solar + wind
    net_power = total_generation - load
    grid_power_kw = 0
    
    time_step_hours = 1 / 3600

    if net_power > 0: # Surplus
        charge_power = min(net_power, BATTERY_MAX_CHARGE_KW)
        energy_to_add = charge_power * time_step_hours
        available_capacity = BATTERY_CAPACITY_KWH - battery_level
        
        if available_capacity > 0:
            actual_energy_added = min(energy_to_add, available_capacity)
            battery_level += actual_energy_added
    elif net_power < 0: # Deficit
        power_needed = abs(net_power)
        
        if battery_level > 0:
            discharge_power = min(power_needed, BATTERY_MAX_DISCHARGE_KW)
            energy_to_draw = discharge_power * time_step_hours
            actual_energy_drawn = min(energy_to_draw, battery_level)
            battery_level -= actual_energy_drawn
            power_needed -= (actual_energy_drawn / time_step_hours)

        if power_needed > 0:
            grid_power_kw = power_needed
            
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
        is_charging = renewable_gen > latest_row["campus_load_kw"] and battery_percentage < 100
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
    total_grid_kwh = sum(row['grid_power_kw'] for row in rows) / 3600
    renewable_usage_percentage = max(0, (1 - (total_grid_kwh / total_load_kwh))) * 100 if total_load_kwh > 0 else 0
    carbon_avoided_kg = (total_load_kwh - total_grid_kwh) * 0.82

    return {
        "renewable_usage_percentage": round(renewable_usage_percentage, 2),
        "total_grid_kwh_24h": round(total_grid_kwh, 2), "carbon_avoided_kg_24h": round(carbon_avoided_kg, 2),
    }

# --- NEW: SERVE FRONTEND ---
@app.get("/")
async def serve_index():
    """Serves the index.html file for the frontend dashboard."""
    return FileResponse('static/index.html', media_type='text/html')

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("Starting FastAPI server...")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

